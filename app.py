from flask import Flask, redirect, request, jsonify, Response
import requests
import os
import json
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import threading
import time
import csv
from io import StringIO

app = Flask(__name__)

# Configuration
SQUARE_API_VERSION = '2025-08-20'
SYNC_INTERVAL_HOURS = 12
SYNC_THRESHOLD_DAYS = 3
TOKEN_REFRESH_DAYS = 25
CUSTOMER_HISTORY_DAYS = 90

class SquareSync:
    def __init__(self):
        self.sheets_client = None
        self._init_sheets_client()
    
    def _init_sheets_client(self):
        """Initialize Google Sheets client"""
        try:
            creds_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
            if not creds_json:
                raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not found")
            
            creds_dict = json.loads(creds_json)
            creds = Credentials.from_service_account_info(
                creds_dict, 
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            self.sheets_client = gspread.authorize(creds)
            
        except Exception as e:
            print(f"❌ Google Sheets init error: {e}")
    
    def _get_sheet(self, sheet_name, create_if_missing=True):
        """Get or create a Google Sheet"""
        try:
            spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
            spreadsheet = self.sheets_client.open_by_key(spreadsheet_id)
            
            try:
                return spreadsheet.worksheet(sheet_name)
            except:
                if create_if_missing:
                    print(f"📝 Creating sheet: {sheet_name}")
                    return spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=30)
                return None
        except Exception as e:
            print(f"❌ Sheet error: {e}")
            return None
    
    def _make_square_request(self, endpoint, access_token, method='GET', data=None):
        """Make Square API request with consistent error handling"""
        base_url = 'https://connect.squareup.com'
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Square-Version': SQUARE_API_VERSION
        }
        
        url = f"{base_url}/{endpoint.lstrip('/')}"
        
        try:
            if method == 'POST':
                response = requests.post(url, headers=headers, json=data)
            else:
                response = requests.get(url, headers=headers, params=data)
            
            return response
        except Exception as e:
            print(f"❌ Square API error: {e}")
            return None
    
    def save_tokens(self, merchant_id, access_token, refresh_token, merchant_name=None):
        """Save or update merchant tokens"""
        sheet = self._get_sheet('tokens')
        if not sheet:
            return False
        
        # Ensure headers exist
        try:
            if not sheet.get_all_values():
                headers = ['merchant_id', 'access_token', 'refresh_token', 'updated_at', 
                          'status', 'merchant_name', 'last_sync', 'total_customers']
                sheet.append_row(headers)
        except:
            pass
        
        records = sheet.get_all_records()
        current_time = datetime.now().isoformat()
        
        # Update existing or add new
        for i, record in enumerate(records, start=2):
            if record.get('merchant_id') == merchant_id:
                # Update existing
                update_data = [access_token, refresh_token, current_time, 'active', 
                             merchant_name or record.get('merchant_name', ''),
                             record.get('last_sync', ''), record.get('total_customers', 0)]
                sheet.update(f'B{i}:H{i}', [update_data])
                print(f"✅ Updated tokens for {merchant_id}")
                return True
        
        # Add new merchant
        new_row = [merchant_id, access_token, refresh_token, current_time, 
                   'active', merchant_name or '', '', 0]
        sheet.append_row(new_row)
        print(f"✅ Added new merchant {merchant_id}")
        return True
    
    def get_tokens(self, merchant_id):
        """Get merchant tokens"""
        sheet = self._get_sheet('tokens', create_if_missing=False)
        if not sheet:
            return None
        
        records = sheet.get_all_records()
        for record in records:
            if record.get('merchant_id') == merchant_id and record.get('status') == 'active':
                return record
        return None
    
    def get_all_merchants(self):
        """Get all active merchants"""
        sheet = self._get_sheet('tokens', create_if_missing=False)
        if not sheet:
            return []
        
        records = sheet.get_all_records()
        merchants = []
        seen = set()
        
        for record in records:
            merchant_id = record.get('merchant_id')
            if record.get('status') == 'active' and merchant_id not in seen:
                seen.add(merchant_id)
                merchants.append(record)
        
        return merchants
    
    def refresh_token(self, merchant_id):
        """Refresh access token"""
        tokens = self.get_tokens(merchant_id)
        if not tokens or not tokens.get('refresh_token'):
            return False
        
        client_id = os.environ.get('SQUARE_CLIENT_ID')
        client_secret = os.environ.get('SQUARE_CLIENT_SECRET')
        
        response = requests.post('https://connect.squareup.com/oauth2/token', data={
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': tokens['refresh_token'],
            'grant_type': 'refresh_token'
        })
        
        if response.status_code == 200:
            token_data = response.json()
            new_access_token = token_data.get('access_token')
            new_refresh_token = token_data.get('refresh_token', tokens['refresh_token'])
            
            if self.save_tokens(merchant_id, new_access_token, new_refresh_token, tokens.get('merchant_name')):
                print(f"✅ Refreshed token for {merchant_id}")
                return True
        
        print(f"❌ Token refresh failed for {merchant_id}")
        return False
    
    def fetch_customers(self, merchant_id, access_token):
        """Fetch customers from Square with 3-month date filtering"""
        print(f"👥 Fetching customers for {merchant_id}")
        
        # Date filter - last 3 months
        cutoff_date = (datetime.now() - timedelta(days=CUSTOMER_HISTORY_DAYS)).isoformat() + 'Z'
        
        search_data = {
            'limit': 100,
            'query': {
                'filter': {
                    'created_at': {'start_at': cutoff_date}
                }
            }
        }
        
        all_customers = []
        cursor = None
        
        while True:
            if cursor:
                search_data['cursor'] = cursor
            
            response = self._make_square_request('v2/customers/search', access_token, 'POST', search_data)
            
            if not response or response.status_code != 200:
                print(f"⚠️ Customer search failed, trying fallback...")
                return self._fetch_customers_fallback(access_token)
            
            data = response.json()
            customers = data.get('customers', [])
            all_customers.extend(self._filter_customers_by_date(customers))
            
            cursor = data.get('cursor')
            if not cursor:
                break
        
        print(f"✅ Fetched {len(all_customers)} customers")
        return all_customers
    
    def _filter_customers_by_date(self, customers):
        """Filter customers by date (created or updated in last year)"""
        cutoff_date = datetime.now() - timedelta(days=CUSTOMER_HISTORY_DAYS)
        filtered = []
        
        for customer in customers:
            include = False
            
            for date_field in ['created_at', 'updated_at']:
                date_str = customer.get(date_field)
                if date_str:
                    try:
                        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        if date_obj.replace(tzinfo=None) >= cutoff_date:
                            include = True
                            break
                    except:
                        continue
            
            if include:
                filtered.append(customer)
        
        return filtered
    
    def _fetch_customers_fallback(self, access_token):
        """Fallback method using regular customers endpoint"""
        print("🔄 Using fallback customer fetch")
        
        all_customers = []
        cursor = None
        
        while True:
            params = {'limit': 100}
            if cursor:
                params['cursor'] = cursor
            
            response = self._make_square_request('v2/customers', access_token, data=params)
            
            if not response or response.status_code != 200:
                break
            
            data = response.json()
            customers = data.get('customers', [])
            all_customers.extend(self._filter_customers_by_date(customers))
            
            cursor = data.get('cursor')
            if not cursor:
                break
        
        return all_customers
    
    def fetch_invoices(self, access_token, customer_ids):
        """Fetch invoices with pagination and proper customer filtering"""
        if not customer_ids:
            return {}
        
        # Get locations
        locations_response = self._make_square_request('v2/locations', access_token)
        if not locations_response or locations_response.status_code != 200:
            return {}
        
        locations = locations_response.json().get('locations', [])
        if not locations:
            return {}
        
        location_ids = [loc['id'] for loc in locations if loc.get('id')]
        
        # Date filter for invoices - last 3 months
        cutoff_date = (datetime.now() - timedelta(days=CUSTOMER_HISTORY_DAYS)).isoformat() + 'Z'
        
        # Search invoices with pagination AND date filtering
        search_data = {
            'limit': 100,
            'query': {
                'filter': {
                    'location_ids': location_ids,
                    'created_at': {'start_at': cutoff_date}  # Added date filter
                }
            }
        }
        
        all_invoices = []
        cursor = None
        
        # Get ALL invoices with pagination
        while True:
            if cursor:
                search_data['cursor'] = cursor
            
            response = self._make_square_request('v2/invoices/search', access_token, 'POST', search_data)
            if not response or response.status_code != 200:
                print(f"Invoice search failed: {response.status_code if response else 'No response'}")
                break
            
            data = response.json()
            invoices = data.get('invoices', [])
            all_invoices.extend(invoices)
            print(f"Got {len(invoices)} invoices (total: {len(all_invoices)})")
            
            cursor = data.get('cursor')
            if not cursor:
                break
            
            # Safety limit
            if len(all_invoices) > 2000:
                break
        
        print(f"Found {len(all_invoices)} total invoices")
        
        if not all_invoices:
            return {}
        
        # Get order IDs from invoices
        order_ids = []
        invoice_by_order = {}
        for invoice in all_invoices:
            order_id = invoice.get('order_id')
            if order_id:
                order_ids.append(order_id)
                invoice_by_order[order_id] = invoice
        
        print(f"Found {len(order_ids)} order IDs from invoices")
        
        # Fetch orders in batches
        order_details = {}
        batch_size = 25
        
        for i in range(0, len(order_ids), batch_size):
            batch_order_ids = order_ids[i:i + batch_size]
            orders_data = {'order_ids': batch_order_ids}
            
            orders_response = self._make_square_request('v2/orders/batch-retrieve', access_token, 'POST', orders_data)
            if orders_response and orders_response.status_code == 200:
                orders = orders_response.json().get('orders', [])
                for order in orders:
                    order_id = order.get('id')
                    order_details[order_id] = order
            
            # Small delay between batches
            time.sleep(0.1)
        
        print(f"Retrieved {len(order_details)} orders")
        
        # Map invoices to customers
        customer_invoices = {}
        customer_id_set = set(customer_ids)
        
        for invoice in all_invoices:
            # Get customer ID from invoice
            customer_id = None
            if 'primary_recipient' in invoice:
                customer_id = invoice['primary_recipient'].get('customer_id')
            
            if customer_id and customer_id in customer_id_set and customer_id not in customer_invoices:
                # Get associated order data
                order_id = invoice.get('order_id')
                order = order_details.get(order_id, {})
                
                # Extract fulfillment details
                fulfillments = order.get('fulfillments', [])
                pickup_date = ''
                delivery_date = ''
                pickup_notes = ''
                delivery_notes = ''
                
                for fulfillment in fulfillments:
                    pickup_details = fulfillment.get('pickup_details', {})
                    delivery_details = fulfillment.get('delivery_details', {})
                    
                    if pickup_details:
                        pickup_date = pickup_details.get('pickup_at', '')
                        pickup_notes = pickup_details.get('note', '')
                        
                    if delivery_details:
                        delivery_date = delivery_details.get('deliver_at', '')
                        delivery_notes = delivery_details.get('note', '')
                
                # Extract invoice data
                payment_requests = invoice.get('payment_requests', [])
                order_money = invoice.get('order', {})
                total_money = order_money.get('total_money', {})
                
                customer_invoices[customer_id] = {
                    'invoice_id': invoice.get('id', ''),
                    'invoice_number': invoice.get('invoice_number', ''),
                    'invoice_created_at': invoice.get('created_at', ''),
                    'invoice_updated_at': invoice.get('updated_at', ''),
                    'invoice_scheduled_at': invoice.get('scheduled_at', ''),
                    'invoice_status': invoice.get('status', ''),
                    'invoice_amount': str(total_money.get('amount', 0)),
                    'due_date': payment_requests[0].get('due_date', '') if payment_requests else '',
                    'order_id': order_id or '',
                    'order_created_at': order.get('created_at', ''),
                    'order_updated_at': order.get('updated_at', ''),
                    'pickup_date': pickup_date,
                    'pickup_notes': pickup_notes,
                    'delivery_date': delivery_date,
                    'delivery_notes': delivery_notes
                }
        
        print(f"Mapped invoice data for {len(customer_invoices)} customers")
        return customer_invoices

    def save_customer_data(self, merchant_id, customers):
        """Save customer data to Google Sheets with expanded invoice fields"""
        sheet_name = f"customers_{merchant_id}"
        sheet = self._get_sheet(sheet_name)
        if not sheet:
            return False
        
        # Clear existing data
        sheet.clear()
        
        # Updated headers with all the invoice/order fields
        headers = [
            'customer_id', 'given_name', 'family_name', 'company_name', 'nickname',
            'email_address', 'phone_number', 'address_line_1', 'address_line_2', 
            'locality', 'administrative_district_level_1', 'postal_code', 'country',
            'created_at', 'updated_at', 'birthday', 'note', 'reference_id',
            'group_ids', 'segment_ids', 'preferences', 'version', 'sync_date',
            # Invoice and Order fields
            'invoice_id', 'invoice_number', 'invoice_created_at', 'invoice_updated_at', 
            'invoice_scheduled_at', 'invoice_status', 'invoice_amount', 'due_date',
            'order_id', 'order_created_at', 'order_updated_at',
            'pickup_date', 'pickup_notes', 'delivery_date', 'delivery_notes'
        ]
        
        # Prepare data
        rows = [headers]
        for customer in customers:
            invoice = customer.get('latest_invoice', {})
            address = customer.get('address', {})
            
            row = [
                customer.get('id', ''),
                customer.get('given_name', ''),
                customer.get('family_name', ''),
                customer.get('company_name', ''),
                customer.get('nickname', ''),
                customer.get('email_address', ''),
                customer.get('phone_number', ''),
                address.get('address_line_1', ''),
                address.get('address_line_2', ''),
                address.get('locality', ''),
                address.get('administrative_district_level_1', ''),
                address.get('postal_code', ''),
                address.get('country', ''),
                customer.get('created_at', ''),
                customer.get('updated_at', ''),
                customer.get('birthday', ''),
                customer.get('note', ''),
                customer.get('reference_id', ''),
                ', '.join(customer.get('group_ids', [])),
                ', '.join(customer.get('segment_ids', [])),
                json.dumps(customer.get('preferences', {})) if customer.get('preferences') else '',
                str(customer.get('version', '')),
                datetime.now().isoformat(),
                # Invoice and Order data
                invoice.get('invoice_id', ''),
                invoice.get('invoice_number', ''),
                invoice.get('invoice_created_at', ''),
                invoice.get('invoice_updated_at', ''),
                invoice.get('invoice_scheduled_at', ''),
                invoice.get('invoice_status', ''),
                invoice.get('invoice_amount', ''),
                invoice.get('due_date', ''),
                invoice.get('order_id', ''),
                invoice.get('order_created_at', ''),
                invoice.get('order_updated_at', ''),
                invoice.get('pickup_date', ''),
                invoice.get('pickup_notes', ''),
                invoice.get('delivery_date', ''),
                invoice.get('delivery_notes', '')
            ]
            rows.append(row)
        
        # Save to sheets
        try:
            range_name = f'A1:{self._get_column_letter(len(headers))}{len(rows)}'
            sheet.update(values=rows, range_name=range_name)
            return True
        except Exception as e:
            print(f"Save error: {e}")
            return False
    
    def _get_column_letter(self, col_num):
        """Convert column number to Excel letter (A, B, ..., AA, AB...)"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(col_num % 26 + ord('A')) + result
            col_num //= 26
        return result
    
    def update_sync_status(self, merchant_id, total_customers):
        """Update last sync time and customer count"""
        sheet = self._get_sheet('tokens', create_if_missing=False)
        if not sheet:
            return False
        
        records = sheet.get_all_records()
        for i, record in enumerate(records, start=2):
            if record.get('merchant_id') == merchant_id:
                current_time = datetime.now().isoformat()
                sheet.update(f'G{i}:H{i}', [[current_time, total_customers]])
                print(f"✅ Updated sync status for {merchant_id}")
                return True
        
        return False
    
    def sync_merchant(self, merchant_id):
        """Complete sync process for one merchant"""
        print(f"🚀 Starting sync for {merchant_id}")
        
        # Get tokens
        tokens = self.get_tokens(merchant_id)
        if not tokens:
            print(f"❌ No tokens found for {merchant_id}")
            return False
        
        # Fetch customers
        customers = self.fetch_customers(merchant_id, tokens['access_token'])
        if not customers:
            print(f"❌ No customers fetched for {merchant_id}")
            return False
        
        print(f"✅ Got {len(customers)} customers")
        
        # Fetch invoices - THIS WAS MISSING!
        customer_ids = [c.get('id') for c in customers if c.get('id')]
        print(f"🔍 About to fetch invoices for {len(customer_ids)} customer IDs")
        
        invoices = self.fetch_invoices(tokens['access_token'], customer_ids)
        print(f"🔍 fetch_invoices returned: {len(invoices) if invoices else 0} mappings")
        
        # Merge invoice data - THIS WAS MISSING!
        customers_with_invoices = 0
        for customer in customers:
            customer_id = customer.get('id')
            if customer_id and customer_id in invoices:
                customer['latest_invoice'] = invoices[customer_id]
                customers_with_invoices += 1
                print(f"✅ Added invoice to customer {customer_id}")
        
        print(f"🔗 Final: {customers_with_invoices} customers have invoice data")
        
        # Save data
        if self.save_customer_data(merchant_id, customers):
            self.update_sync_status(merchant_id, len(customers))
            print(f"✅ Sync complete for {merchant_id}: {len(customers)} customers")
            return True
        
        print(f"❌ Failed to save data for {merchant_id}")
        return False

    def should_sync(self, last_sync):
        """Check if merchant needs syncing"""
        if not last_sync:
            return True
        
        try:
            last_sync_date = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
            days_since = (datetime.now() - last_sync_date.replace(tzinfo=None)).days
            return days_since >= SYNC_THRESHOLD_DAYS
        except:
            return True
    
    def should_refresh_token(self, updated_at):
        """Check if token needs refresh"""
        if not updated_at:
            return True
        
        try:
            token_date = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
            days_old = (datetime.now() - token_date.replace(tzinfo=None)).days
            return days_old >= TOKEN_REFRESH_DAYS
        except:
            return True

# Global sync instance
sync = SquareSync()

@app.route('/')
def home():
    return '''
    <style>
        body { font-family: Arial, sans-serif; margin: 50px; text-align: center; }
        .btn { background: #007bff; color: white; padding: 15px 30px; text-decoration: none; 
               border-radius: 8px; margin: 10px; display: inline-block; }
        .btn:hover { background: #0056b3; }
        .btn-success { background: #28a745; }
    </style>
    <h1>🔄 Square Customer Data Sync</h1>
    <p>Automatically sync customer data from Square to Google Sheets.</p>
    <a href="/signin" class="btn">Connect Your Square Account</a>
    <a href="/dashboard" class="btn btn-success">View Dashboard</a>
    '''

@app.route('/signin')
def signin():
    """Initiate Square OAuth with comprehensive debugging"""
    client_id = os.environ.get('SQUARE_CLIENT_ID')
    redirect_uri = os.environ.get('SQUARE_REDIRECT_URI')
    
    print(f"=== SIGNIN DEBUG ===")
    print(f"Client ID: {client_id[:10] + '...' if client_id else 'None'}")
    print(f"Redirect URI: {redirect_uri}")
    print(f"Request URL: {request.url}")
    print(f"Request method: {request.method}")
    
    if not client_id or not redirect_uri:
        error_msg = f'Error: Missing Square configuration - Client ID: {"SET" if client_id else "MISSING"}, Redirect URI: {"SET" if redirect_uri else "MISSING"}'
        print(f"ERROR: {error_msg}")
        return error_msg, 500
    
    scope = 'CUSTOMERS_READ MERCHANT_PROFILE_READ INVOICES_READ ORDERS_READ PAYMENTS_READ APPOINTMENTS_READ'
    auth_url = (f'https://connect.squareup.com/oauth2/authorize'
               f'?client_id={client_id}&redirect_uri={redirect_uri}'
               f'&scope={scope}&response_type=code')
    
    print(f"Auth URL: {auth_url}")
    print(f"About to redirect...")
    
    return redirect(auth_url)

# Also add some debugging to the OAuth callback
@app.route('/oauth2callback')
def oauth2callback():
    """Handle Square OAuth callback with debugging"""
    print(f"=== OAUTH CALLBACK DEBUG ===")
    print(f"Full URL: {request.url}")
    print(f"Args: {request.args}")
    
    code = request.args.get('code')
    error = request.args.get('error')
    
    print(f"Code: {code[:10] + '...' if code else 'None'}")
    print(f"Error: {error}")
    
    if error:
        print(f"Authorization denied: {error}")
        return f'''
        <div style="max-width: 600px; margin: 50px auto; padding: 30px; background: #f8d7da; 
             border: 1px solid #f5c6cb; border-radius: 8px; font-family: Arial;">
            <h1 style="color: #721c24;">❌ Authorization Error</h1>
            <p><strong>Error:</strong> {error}</p>
            <p><strong>Description:</strong> {request.args.get('error_description', 'No description provided')}</p>
            <a href="/" style="background: #007bff; color: white; padding: 10px 20px; 
               text-decoration: none; border-radius: 5px;">← Back to Home</a>
        </div>
        ''', 400
        
    if not code:
        print("ERROR: No authorization code received")
        return '''
        <div style="max-width: 600px; margin: 50px auto; padding: 30px; background: #f8d7da; 
             border: 1px solid #f5c6cb; border-radius: 8px; font-family: Arial;">
            <h1 style="color: #721c24;">❌ Missing Authorization Code</h1>
            <p>No authorization code was received from Square.</p>
            <p>This could mean:</p>
            <ul>
                <li>The user denied permission</li>
                <li>There's an issue with the redirect URI configuration</li>
                <li>Network connectivity problems</li>
            </ul>
            <a href="/signin" style="background: #28a745; color: white; padding: 10px 20px; 
               text-decoration: none; border-radius: 5px;">Try Again</a>
            <a href="/" style="background: #007bff; color: white; padding: 10px 20px; 
               text-decoration: none; border-radius: 5px; margin-left: 10px;">← Back to Home</a>
        </div>
        ''', 400
    
    # Exchange code for tokens
    client_id = os.environ.get('SQUARE_CLIENT_ID')
    client_secret = os.environ.get('SQUARE_CLIENT_SECRET')
    redirect_uri = os.environ.get('SQUARE_REDIRECT_URI')
    
    print(f"Exchanging code for tokens...")
    print(f"Client ID: {client_id[:10] + '...' if client_id else 'None'}")
    print(f"Client Secret: {'SET' if client_secret else 'MISSING'}")
    
    response = requests.post('https://connect.squareup.com/oauth2/token', data={
        'client_id': client_id,
        'client_secret': client_secret,
        'code': code,
        'grant_type': 'authorization_code',
        'redirect_uri': redirect_uri
    })
    
    print(f"Token exchange response status: {response.status_code}")
    print(f"Token exchange response: {response.text}")
    
    if response.status_code != 200:
        return f'''
        <div style="max-width: 600px; margin: 50px auto; padding: 30px; background: #f8d7da; 
             border: 1px solid #f5c6cb; border-radius: 8px; font-family: Arial;">
            <h1 style="color: #721c24;">❌ Token Exchange Failed</h1>
            <p><strong>Status:</strong> {response.status_code}</p>
            <p><strong>Response:</strong> {response.text}</p>
            <a href="/signin" style="background: #28a745; color: white; padding: 10px 20px; 
               text-decoration: none; border-radius: 5px;">Try Again</a>
        </div>
        ''', response.status_code
    
    token_data = response.json()
    merchant_id = token_data.get('merchant_id')
    access_token = token_data.get('access_token')
    refresh_token = token_data.get('refresh_token')
    
    print(f"Successfully got tokens for merchant: {merchant_id}")
    
    # Continue with the rest of your existing oauth2callback logic...
    # Get merchant name
    merchant_name = "Unknown"
    merchant_response = sync._make_square_request('v2/merchants', access_token)
    if merchant_response and merchant_response.status_code == 200:
        merchant_data = merchant_response.json()
        merchants = merchant_data.get('merchant', [])
        if merchants:
            merchant_name = merchants[0].get('business_name', 'Unknown')
    
    # Save tokens and trigger initial sync
    if sync.save_tokens(merchant_id, access_token, refresh_token, merchant_name):
        # Start background sync
        threading.Thread(target=sync.sync_merchant, args=(merchant_id,), daemon=True).start()
        
        return f'''
        <div style="max-width: 600px; margin: 50px auto; padding: 30px; background: white; 
             border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); font-family: Arial;">
            <h1 style="color: #28a745; text-align: center;">✅ Connected Successfully!</h1>
            <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <p><strong>Business:</strong> {merchant_name}</p>
                <p><strong>Merchant ID:</strong> {merchant_id}</p>
                <p><strong>Status:</strong> Initial sync running in background</p>
            </div>
            <div style="text-align: center;">
                <a href="/dashboard" style="background: #007bff; color: white; padding: 12px 24px; 
                   text-decoration: none; border-radius: 5px;">View Dashboard</a>
            </div>
        </div>
        '''
    else:
        return 'Failed to save tokens', 500

@app.route('/dashboard')
def dashboard():
    """Main dashboard"""
    merchants = sync.get_all_merchants()
    
    if not merchants:
        return '''
        <h1>🔄 Square Sync Dashboard</h1>
        <div style="text-align: center; margin: 50px;">
            <h3>No merchants connected yet</h3>
            <a href="/signin" style="background: #28a745; color: white; padding: 15px 30px; 
               text-decoration: none; border-radius: 8px;">Connect Square Account</a>
        </div>
        '''
    
    # Build merchant table
    table_rows = ""
    for merchant in merchants:
        merchant_id = merchant['merchant_id']
        name = merchant.get('merchant_name', 'Unknown')
        customers = merchant.get('total_customers', 0)
        last_sync = merchant.get('last_sync', 'Never')
        
        # Format last sync
        sync_display = 'Never'
        if last_sync and last_sync != 'Never':
            try:
                sync_date = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
                days_ago = (datetime.now() - sync_date.replace(tzinfo=None)).days
                sync_display = f'{days_ago} days ago' if days_ago > 0 else 'Today'
            except:
                pass
        
        table_rows += f'''
        <tr>
            <td>{name}</td>
            <td><code>{merchant_id}</code></td>
            <td>{customers:,}</td>
            <td>{sync_display}</td>
            <td>
                <a href="/api/sync/{merchant_id}" style="background: #28a745; color: white; 
                   padding: 8px 12px; text-decoration: none; border-radius: 4px; margin: 2px;">Sync</a>
                <a href="/api/export/{merchant_id}" style="background: #007bff; color: white; 
                   padding: 8px 12px; text-decoration: none; border-radius: 4px; margin: 2px;">Export</a>
            </td>
        </tr>
        '''
    
    return f'''
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
    
    <h1>🔄 Square Sync Dashboard</h1>
    
    <div style="background: #e9ecef; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
        <h3>📊 Status</h3>
        <p><strong>Connected Merchants:</strong> {len(merchants)}</p>
        <p><strong>Auto-sync:</strong> Every {SYNC_INTERVAL_HOURS} hours</p>
    </div>
    
    <table>
        <tr>
            <th>Business Name</th>
            <th>Merchant ID</th>
            <th>Customers</th>
            <th>Last Sync</th>
            <th>Actions</th>
        </tr>
        {table_rows}
    </table>
    
    <div style="margin-top: 20px;">
        <a href="/signin" style="background: #28a745; color: white; padding: 10px 20px; 
           text-decoration: none; border-radius: 5px; margin: 5px;">➕ Connect New Account</a>
        <a href="/api/force-sync-all" style="background: #ffc107; color: black; padding: 10px 20px; 
           text-decoration: none; border-radius: 5px; margin: 5px;">🔄 Sync All</a>
        <a href="/api/lookup-customer" style="background: #17a2b8; color: white; padding: 10px 20px; 
           text-decoration: none; border-radius: 5px; margin: 5px;">🔍 Customer Lookup</a>
    </div>
    '''

@app.route('/api/sync/<merchant_id>')
def manual_sync(merchant_id):
    """Manual sync trigger"""
    success = sync.sync_merchant(merchant_id)
    
    if success:
        tokens = sync.get_tokens(merchant_id)
        customer_count = tokens.get('total_customers', 0) if tokens else 0
        
        return f'''
        <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
            <h2 style="color: #28a745;">✅ Sync Complete!</h2>
            <p><strong>Customers synced:</strong> {customer_count:,}</p>
            <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <a href="/dashboard" style="background: #007bff; color: white; padding: 12px 24px; 
               text-decoration: none; border-radius: 5px;">Back to Dashboard</a>
        </div>
        '''
    else:
        return f'''
        <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
            <h2 style="color: #dc3545;">❌ Sync Failed</h2>
            <p>Check logs for details</p>
            <a href="/dashboard" style="background: #007bff; color: white; padding: 12px 24px; 
               text-decoration: none; border-radius: 5px;">Back to Dashboard</a>
        </div>
        ''', 500

@app.route('/api/export/<merchant_id>')
def export_csv(merchant_id):
    """Export customer data as CSV"""
    try:
        sheet_name = f"customers_{merchant_id}"
        sheet = sync._get_sheet(sheet_name, create_if_missing=False)
        
        if not sheet:
            return f'No data found for {merchant_id}. Try syncing first.', 404
        
        data = sheet.get_all_values()
        if not data:
            return f'Sheet is empty for {merchant_id}', 404
        
        # Create CSV
        output = StringIO()
        writer = csv.writer(output)
        writer.writerows(data)
        
        filename = f'customers_{merchant_id}_{datetime.now().strftime("%Y%m%d")}.csv'
        
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
        
    except Exception as e:
        return f'Export failed: {str(e)}', 500

@app.route('/api/force-sync-all')
def force_sync_all():
    """Force sync all merchants"""
    merchants = sync.get_all_merchants()
    results = []
    
    for merchant in merchants:
        merchant_id = merchant['merchant_id']
        name = merchant.get('merchant_name', 'Unknown')
        
        if sync.sync_merchant(merchant_id):
            results.append(f"✅ {name}")
        else:
            results.append(f"❌ {name}")
    
    return f'''
    <h2>🔄 Bulk Sync Results</h2>
    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; font-family: monospace;">
        {("<br>".join(results))}
    </div>
    <a href="/dashboard">Back to Dashboard</a>
    '''

def background_sync():
    """Background sync task"""
    print(f"🚀 Background sync started - every {SYNC_INTERVAL_HOURS} hours")
    
    while True:
        try:
            merchants = sync.get_all_merchants()
            synced = refreshed = 0
            
            for merchant in merchants:
                merchant_id = merchant['merchant_id']
                
                # Refresh token if needed
                if sync.should_refresh_token(merchant.get('updated_at')):
                    if sync.refresh_token(merchant_id):
                        refreshed += 1
                
                # Sync if needed
                if sync.should_sync(merchant.get('last_sync')):
                    if sync.sync_merchant(merchant_id):
                        synced += 1
                    time.sleep(10)  # Rate limiting
            
            print(f"🎉 Background cycle: {refreshed} tokens refreshed, {synced} merchants synced")
            time.sleep(SYNC_INTERVAL_HOURS * 3600)  # Sleep until next cycle
            
        except Exception as e:
            print(f"❌ Background sync error: {e}")
            time.sleep(3600)  # Sleep 1 hour on error

@app.route('/api/cron-sync')
def cron_sync():
    """External cron endpoint"""
    auth_token = request.headers.get('Authorization')
    expected_token = f"Bearer {os.environ.get('CRON_TOKEN')}"
    
    if auth_token != expected_token:
        return jsonify({'error': 'Unauthorized'}), 401
    
    merchants = sync.get_all_merchants()
    synced_count = refreshed_count = 0
    results = []
    
    for merchant in merchants:
        merchant_id = merchant['merchant_id']
        name = merchant.get('merchant_name', 'Unknown')
        
        # Refresh token if needed
        if sync.should_refresh_token(merchant.get('updated_at')):
            if sync.refresh_token(merchant_id):
                refreshed_count += 1
        
        # Sync if needed
        if sync.should_sync(merchant.get('last_sync')):
            if sync.sync_merchant(merchant_id):
                synced_count += 1
                results.append(f"✅ {name}")
            else:
                results.append(f"❌ {name}")
        else:
            results.append(f"⏭️ {name} (recently synced)")
    
    return jsonify({
        'status': 'completed',
        'synced_count': synced_count,
        'refreshed_tokens': refreshed_count,
        'total_merchants': len(merchants),
        'results': results,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'merchants_connected': len(sync.get_all_merchants())
    })

@app.route('/api/lookup-customer', methods=['GET', 'POST'])
def lookup_customer():
    """Direct API lookup for customer invoices"""
    if request.method == 'POST':
        merchant_id = request.form.get('merchant_id')
        customer_id = request.form.get('customer_id')
        
        if not merchant_id or not customer_id:
            return '''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
                <h2 style="color: #dc3545;">❌ Error</h2>
                <p>Both Merchant ID and Customer ID are required</p>
                <a href="/api/lookup-customer" class="btn">Try Again</a>
            </div>
            '''
        
        # Get merchant tokens
        tokens = sync.get_tokens(merchant_id)
        if not tokens or 'access_token' not in tokens:
            return '''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
                <h2 style="color: #dc3545;">❌ Error</h2>
                <p>Invalid merchant ID or merchant not connected</p>
                <a href="/api/lookup-customer" class="btn">Try Again</a>
            </div>
            '''
        
        access_token = tokens['access_token']
        
        # Get customer details first
        customer_response = sync._make_square_request(f'v2/customers/{customer_id}', access_token)
        if not customer_response or customer_response.status_code != 200:
            return '''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
                <h2 style="color: #dc3545;">❌ Error</h2>
                <p>Customer not found</p>
                <a href="/api/lookup-customer" class="btn">Try Again</a>
            </div>
            '''
        
        customer_data = customer_response.json().get('customer', {})
        
        # Get all locations for the merchant
        locations_response = sync._make_square_request('v2/locations', access_token)
        if not locations_response or locations_response.status_code != 200:
            return 'Failed to get merchant locations', 500
        
        location_ids = [loc['id'] for loc in locations_response.json().get('locations', [])]
        
        # Search for invoices with customer filter
        search_data = {
            'query': {
                'filter': {
                    'customer_ids': [customer_id],
                    'location_ids': location_ids
                }
            }
        }
        
        invoices_response = sync._make_square_request('v2/invoices/search', 
                                                    access_token, 
                                                    'POST', 
                                                    search_data)
        
        if not invoices_response or invoices_response.status_code != 200:
            return 'Failed to fetch invoices', 500
        
        invoices = invoices_response.json().get('invoices', [])
        
        # Get associated orders
        order_details = {}
        for invoice in invoices:
            order_id = invoice.get('order_id')
            if order_id:
                order_response = sync._make_square_request(f'v2/orders/{order_id}', access_token)
                if order_response and order_response.status_code == 200:
                    order_details[order_id] = order_response.json().get('order', {})
        
        # Build the display
        invoice_rows = ""
        for invoice in invoices:
            order_id = invoice.get('order_id')
            order = order_details.get(order_id, {})
            
            # Get order amount
            total_money = order.get('total_money', {})
            amount = float(total_money.get('amount', 0)) / 100 if total_money else 0
            
            # Get sale or service date from order metadata
            sale_or_service_date = 'N/A'
            if 'metadata' in order:
                sale_or_service_date = order['metadata'].get('sale_or_service_date', 'N/A')

            # Get fulfillment details
            fulfillments = order.get('fulfillments', [])
            pickup_date = delivery_date = pickup_notes = delivery_notes = 'N/A'
            
            for fulfillment in fulfillments:
                if 'pickup_details' in fulfillment:
                    pickup = fulfillment['pickup_details']
                    pickup_date = pickup.get('pickup_at', 'N/A')
                    pickup_notes = pickup.get('note', 'N/A')
                if 'delivery_details' in fulfillment:
                    delivery = fulfillment['delivery_details']
                    delivery_date = delivery.get('deliver_at', 'N/A')
                    delivery_notes = delivery.get('note', 'N/A')
            
            invoice_rows += f'''
            <tr>
                <td>{invoice.get('id', 'N/A')}</td>
                <td>{invoice.get('invoice_number', 'N/A')}</td>
                <td>${amount:.2f}</td>
                <td>{invoice.get('status', 'N/A')}</td>
                <td>{invoice.get('created_at', 'N/A')}</td>
                <td>{sale_or_service_date}</td>
                <td>{pickup_date}</td>
                <td>{delivery_date}</td>
            </tr>
            '''
        
        return f'''
        <div style="max-width: 1000px; margin: 50px auto; padding: 30px;">
            <h2>📋 Customer Details</h2>
            <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
                <p><strong>Name:</strong> {customer_data.get('given_name', '')} {customer_data.get('family_name', '')}</p>
                <p><strong>Email:</strong> {customer_data.get('email_address', 'N/A')}</p>
                <p><strong>Phone:</strong> {customer_data.get('phone_number', 'N/A')}</p>
                <p><strong>Customer ID:</strong> {customer_id}</p>
            </div>
            
            <h3>📊 Invoice History ({len(invoices)} invoices found)</h3>
            <table style="width: 100%; border-collapse: collapse; margin-top: 20px;">
                <tr>
                    <th>Invoice ID</th>
                    <th>Invoice Number</th>
                    <th>Amount</th>
                    <th>Status</th>
                    <th>Created Date</th>
                    <th>Sale/Service Date</th>
                    <th>Pickup Date</th>
                    <th>Delivery Date</th>
                </tr>
                {invoice_rows}
            </table>
            
            <div style="margin-top: 20px;">
                <a href="/api/lookup-customer" style="background: #007bff; color: white; 
                   padding: 10px 20px; text-decoration: none; border-radius: 5px;">New Lookup</a>
                <a href="/dashboard" style="background: #6c757d; color: white; padding: 10px 20px; 
                   text-decoration: none; border-radius: 5px; margin-left: 10px;">Back to Dashboard</a>
            </div>
        </div>
        '''
    
    # GET request - show form
    merchants = sync.get_all_merchants()
    merchant_options = ''.join([
        f'<option value="{m["merchant_id"]}">{m.get("merchant_name", "Unknown")} ({m["merchant_id"]})</option>'
        for m in merchants
    ])
    
    return f'''
    <div style="max-width: 600px; margin: 50px auto; padding: 30px;">
        <h2>🔍 Customer Invoice Lookup</h2>
        <form method="POST" style="background: #f8f9fa; padding: 20px; border-radius: 8px;">
            <div style="margin-bottom: 15px;">
                <label style="display: block; margin-bottom: 5px;">
                    <strong>Select Merchant:</strong>
                </label>
                <select name="merchant_id" required style="width: 100%; padding: 8px; border-radius: 4px;">
                    <option value="">-- Select Merchant --</option>
                    {merchant_options}
                </select>
            </div>
            
            <div style="margin-bottom: 15px;">
                <label style="display: block; margin-bottom: 5px;">
                    <strong>Customer ID:</strong>
                </label>
                <input type="text" name="customer_id" required 
                       style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ced4da;">
            </div>
            
            <button type="submit" style="background: #007bff; color: white; padding: 10px 20px; 
                    border: none; border-radius: 5px; cursor: pointer;">
                Look Up Customer
            </button>
        </form>
        
        <div style="margin-top: 20px; text-align: center;">
            <a href="/dashboard" style="color: #6c757d; text-decoration: none;">Back to Dashboard</a>
        </div>
    </div>
    '''

# Expose sync methods for backwards compatibility
def get_tokens_from_sheets(merchant_id):
    return sync.get_tokens(merchant_id)

def save_tokens_to_sheets(merchant_id, access_token, refresh_token, merchant_name=None):
    return sync.save_tokens(merchant_id, access_token, refresh_token, merchant_name)

def get_all_active_merchants():
    return sync.get_all_merchants()

def sync_merchant_customers(merchant_id, days_back=365):
    return sync.sync_merchant(merchant_id)

if __name__ == '__main__':
    # Start background sync
    sync_thread = threading.Thread(target=background_sync, daemon=True)
    sync_thread.start()
    
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))