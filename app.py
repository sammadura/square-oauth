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
SYNC_THRESHOLD_DAYS = 1
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
    
    def save_tokens(self, merchant_id, access_token, refresh_token, merchant_name=None, location_ids=None):
        """Save or update merchant tokens"""
        sheet = self._get_sheet('tokens')
        if not sheet:
            return False
        
        # Ensure headers exist
        try:
            if not sheet.get_all_values():
                headers = ['merchant_id', 'access_token', 'refresh_token', 'updated_at', 
                          'status', 'merchant_name', 'last_sync', 'total_customers', 'location_ids']
                sheet.append_row(headers)
        except:
            pass
        
        records = sheet.get_all_records()
        current_time = datetime.now().isoformat()
        location_ids_str = ','.join(location_ids) if location_ids else ''
        
        # Update existing or add new
        for i, record in enumerate(records, start=2):
            if record.get('merchant_id') == merchant_id:
                # Update existing
                update_data = [access_token, refresh_token, current_time, 'active', 
                             merchant_name or record.get('merchant_name', ''),
                             record.get('last_sync', ''), record.get('total_customers', 0),
                             location_ids_str or record.get('location_ids', '')]
                sheet.update(f'B{i}:I{i}', [update_data])
                print(f"✅ Updated tokens for {merchant_id}")
                return True
        
        # Add new merchant
        new_row = [merchant_id, access_token, refresh_token, current_time, 
                   'active', merchant_name or '', '', 0, location_ids_str]
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
            
            # Keep existing location_ids when refreshing tokens
            existing_location_ids = tokens.get('location_ids', '').split(',') if tokens.get('location_ids') else None
            if self.save_tokens(merchant_id, new_access_token, new_refresh_token, 
                              tokens.get('merchant_name'), existing_location_ids):
                print(f"✅ Refreshed token for {merchant_id}")
                return True
        
        print(f"❌ Token refresh failed for {merchant_id}")
        return False
    
    def fetch_customers_simple(self, access_token):
        """Fetch customers - limit 100, desc by date"""
        search_data = {
            "limit": 100,
            "query": {
                "sort": {
                    "field": "CREATED_AT", 
                    "order": "DESC"
                }
            }
        }
        
        response = self._make_square_request('v2/customers/search', access_token, 'POST', search_data)
        
        if response and response.status_code == 200:
            customers = response.json().get('customers', [])
            print(f"✅ Fetched {len(customers)} customers")
            return customers
        
        print(f"❌ Customer fetch failed")
        return []

    def fetch_invoices_simple(self, access_token, merchant_id):
        """Fetch invoices with better location error handling"""
        # Always fetch fresh location IDs first to avoid using stale ones
        fresh_location_ids = self.fetch_locations(access_token)
        
        if not fresh_location_ids:
            print("No valid location IDs found, skipping invoices")
            return []
        
        # Update stored location IDs for future use
        tokens = self.get_tokens(merchant_id)
        if tokens:
            self.save_tokens(merchant_id, tokens['access_token'], tokens['refresh_token'], 
                        tokens.get('merchant_name'), fresh_location_ids)
        
        search_data = {
            "limit": 200,  # Changed from 100 to 200
            "query": {
                "filter": {"location_ids": fresh_location_ids},
                "sort": {"field": "INVOICE_SORT_DATE", "order": "DESC"}
            }
        }
        
        response = self._make_square_request('v2/invoices/search', access_token, 'POST', search_data)
        
        if response and response.status_code == 200:
            invoices = response.json().get('invoices', [])
            print(f"✅ Fetched {len(invoices)} invoices")
            return invoices
        
        print(f"❌ Invoice fetch failed: {response.status_code if response else 'No response'}")
        if response:
            print(f"Response: {response.text}")
        return []

    def fetch_orders_simple(self, access_token, merchant_id):
        """Fetch orders with permission error handling"""
        location_ids = self._get_location_ids(merchant_id, access_token)
        if not location_ids:
            return []
        
        search_data = {
            "limit": 500,  # Changed from 100 to 500
            "location_ids": location_ids,
            "query": {
                "sort": {"sort_field": "CREATED_AT", "sort_order": "DESC"}
            }
        }
        
        response = self._make_square_request('v2/orders/search', access_token, 'POST', search_data)
        
        # Handle permission errors gracefully
        if response and response.status_code == 403:
            print("Orders permission denied - continuing without orders")
            return []  # Return empty list instead of failing
        
        if response and response.status_code == 200:
            orders = response.json().get('orders', [])
            print(f"✅ Fetched {len(orders)} orders")
            return orders
        
        print(f"❌ Order fetch failed: {response.status_code if response else 'No response'}")
        return []

    def save_json_data(self, merchant_id, data_type, data):
        """Save data to Google Sheets in a more reliable way"""
        sheet_name = f"{merchant_id}_{data_type}"
        sheet = self._get_sheet(sheet_name)
        if not sheet:
            return False
        
        try:
            # Clear existing data
            sheet.clear()
            
            # For customers, save in a tabular format
            if data_type == 'customers' and data:
                # Extract customer fields
                headers = ['id', 'given_name', 'family_name', 'email', 'phone_number', 
                        'company_name', 'created_at', 'updated_at', 'birthday', 'note']
                
                rows = [headers]
                for customer in data:
                    row = [
                        customer.get('id', ''),
                        customer.get('given_name', ''),
                        customer.get('family_name', ''),
                        customer.get('email_address', ''),
                        customer.get('phone_number', ''),
                        customer.get('company_name', ''),
                        customer.get('created_at', ''),
                        customer.get('updated_at', ''),
                        customer.get('birthday', ''),
                        customer.get('note', '')
                    ]
                    rows.append(row)
                
                # Update in batches to avoid API limits
                batch_size = 100
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    if i == 0:
                        # First batch includes headers
                        sheet.update(f'A{i+1}:J{i+len(batch)}', batch)
                    else:
                        # Subsequent batches append data
                        sheet.append_rows(batch[1:])  # Skip header row in subsequent batches
                
                print(f"✅ Saved {len(data)} {data_type} records")
                return True
                
            elif data_type == 'invoices' and data:
                # Save invoices with useful fields - up to 200 records
                headers = ['id', 'customer_id', 'sale_or_service_date', 'invoice_number', 
                        'title', 'status', 'total_amount', 'created_at']
                rows = [headers]
                
                for invoice in data[:200]:  # Increased from 100 to 200
                    # Get total amount from payment_requests
                    total_money = {}
                    payment_requests = invoice.get('payment_requests', [])
                    if payment_requests:
                        total_money = payment_requests[0].get('total_money', {})
                    
                    amount_str = ''
                    if total_money.get('amount'):
                        amount_str = f"{total_money.get('amount', 0)/100:.2f} {total_money.get('currency', 'USD')}"
                    
                    row = [
                        invoice.get('id', ''),
                        invoice.get('primary_recipient', {}).get('customer_id', ''),
                        invoice.get('sale_or_service_date', ''),
                        invoice.get('invoice_number', ''),
                        invoice.get('title', ''),
                        invoice.get('status', ''),
                        amount_str,
                        invoice.get('created_at', '')
                    ]
                    rows.append(row)
                
                # Update in batches for large datasets
                batch_size = 100
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    if i == 0:
                        # First batch includes headers
                        end_col = self._get_column_letter(len(headers))
                        sheet.update(f'A{i+1}:{end_col}{i+len(batch)}', batch)
                    else:
                        # Subsequent batches append data
                        sheet.append_rows(batch[1:])
                
                print(f"✅ Saved {min(len(data), 200)} {data_type} records")
                return True
                
            elif data_type == 'orders' and data:
                # Save orders with useful fields - up to 500 records
                headers = ['id', 'customer_id', 'line_item_notes', 'state', 
                        'total_amount', 'source', 'created_at', 'location_id']
                rows = [headers]
                
                for order in data[:500]:  # Increased from 100 to 500
                    # Extract notes from line_items
                    line_items = order.get('line_items', [])
                    notes = []
                    for item in line_items:
                        note = item.get('note', '')
                        if note:
                            notes.append(note)
                    
                    # Join all notes with semicolon separator
                    combined_notes = '; '.join(notes) if notes else ''
                    
                    # Get total money
                    total_money = order.get('total_money', {})
                    amount_str = ''
                    if total_money.get('amount'):
                        amount_str = f"{total_money.get('amount', 0)/100:.2f} {total_money.get('currency', 'USD')}"
                    
                    # Get source name
                    source = order.get('source', {})
                    source_name = source.get('name', '')
                    
                    row = [
                        order.get('id', ''),
                        order.get('customer_id', ''),
                        combined_notes,
                        order.get('state', ''),
                        amount_str,
                        source_name,
                        order.get('created_at', ''),
                        order.get('location_id', '')
                    ]
                    rows.append(row)
                
                # Update in batches for large datasets
                batch_size = 100
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    if i == 0:
                        # First batch includes headers
                        end_col = self._get_column_letter(len(headers))
                        sheet.update(f'A{i+1}:{end_col}{i+len(batch)}', batch)
                    else:
                        # Subsequent batches append data
                        sheet.append_rows(batch[1:])
                
                print(f"✅ Saved {min(len(data), 500)} {data_type} records")
                return True
            
            return False
                
        except Exception as e:
            print(f"❌ Save error for {data_type}: {str(e)}")
            return False

    def _get_location_ids(self, merchant_id, access_token):
        """Helper to get location IDs"""
        tokens = self.get_tokens(merchant_id)
        if tokens and tokens.get('location_ids'):
            return [l.strip() for l in tokens['location_ids'].split(',') if l.strip()]
        
        return self.fetch_locations(access_token)

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
        """Simplified sync with better error handling"""
        print(f"🔄 Starting sync for {merchant_id}")
        
        tokens = self.get_tokens(merchant_id)
        if not tokens:
            print(f"❌ No tokens found for {merchant_id}")
            return False
        
        access_token = tokens['access_token']
        success_count = 0
        
        # Fetch and save customers
        try:
            customers = self.fetch_customers_simple(access_token)
            if customers:
                if self.save_json_data(merchant_id, 'customers', customers):
                    success_count += 1
                    print(f"✅ Customers: {len(customers)} saved")
            else:
                print("⚠️ No customers fetched")
        except Exception as e:
            print(f"❌ Customer sync error: {e}")
        
        # Fetch and save invoices (with fresh location IDs)
        try:
            invoices = self.fetch_invoices_simple(access_token, merchant_id)
            if invoices:
                if self.save_json_data(merchant_id, 'invoices', invoices):
                    success_count += 1
                    print(f"✅ Invoices: {len(invoices)} saved")
            else:
                print("⚠️ No invoices fetched")
        except Exception as e:
            print(f"❌ Invoice sync error: {e}")
        
        # Try orders but don't fail if no permission
        try:
            orders = self.fetch_orders_simple(access_token, merchant_id)
            if orders:
                if self.save_json_data(merchant_id, 'orders', orders):
                    success_count += 1
                    print(f"✅ Orders: {len(orders)} saved")
            else:
                print("⚠️ No orders fetched (may lack permission)")
        except Exception as e:
            print(f"⚠️ Order sync skipped: {e}")
        
        # Update sync status if at least one data type was saved
        if success_count > 0:
            total_records = len(customers) if 'customers' in locals() else 0
            self.update_sync_status(merchant_id, total_records)
            print(f"✅ Sync completed: {success_count}/3 data types saved")
            return True
        
        print(f"❌ Sync failed - no data saved for {merchant_id}")
        return False

    def clear_location_ids(self, merchant_id):
        """Clear stored location IDs to force refresh"""
        tokens = self.get_tokens(merchant_id)
        if tokens:
            return self.save_tokens(
                merchant_id, 
                tokens['access_token'], 
                tokens['refresh_token'], 
                tokens.get('merchant_name'),
                []  # Clear location IDs
            )
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
    
    def fetch_locations(self, access_token):
        """Fetch merchant locations"""
        print("📍 Fetching merchant locations")
        
        locations_response = self._make_square_request('v2/locations', access_token)
        
        if not locations_response or locations_response.status_code != 200:
            print(f"❌ Failed to get locations: {locations_response.status_code if locations_response else 'No response'}")
            return []
        
        locations_data = locations_response.json()
        locations = locations_data.get('locations', [])
        
        location_ids = [loc.get('id') for loc in locations if loc.get('id')]
        location_names = [loc.get('name', 'Unnamed') for loc in locations]
        
        print(f"✅ Found {len(location_ids)} locations: {location_names}")
        return location_ids

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
    # Get merchant name and locations
    merchant_name = "Unknown"
    location_ids = []
    
    merchant_response = sync._make_square_request('v2/merchants', access_token)
    if merchant_response and merchant_response.status_code == 200:
        merchant_data = merchant_response.json()
        merchants = merchant_data.get('merchant', [])
        if merchants:
            merchant_name = merchants[0].get('business_name', 'Unknown')
    
    # Fetch locations
    location_ids = sync.fetch_locations(access_token)
    
    # Save tokens and trigger initial sync
    if sync.save_tokens(merchant_id, access_token, refresh_token, merchant_name, location_ids):
        # Start background sync
        threading.Thread(target=sync.sync_merchant, args=(merchant_id,), daemon=True).start()
        
        return f'''
        <div style="max-width: 600px; margin: 50px auto; padding: 30px; background: white; 
             border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); font-family: Arial;">
            <h1 style="color: #28a745; text-align: center;">✅ Connected Successfully!</h1>
            <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <p><strong>Business:</strong> {merchant_name}</p>
                <p><strong>Merchant ID:</strong> {merchant_id}</p>
                <p><strong>Locations:</strong> {len(location_ids)} found</p>
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
        location_ids = merchant.get('location_ids', '')
        location_count = len([l for l in location_ids.split(',') if l.strip()]) if location_ids else 0
        
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
            <td>{location_count}</td>
            <td>{sync_display}</td>
            <td>
                <a href="/api/sync/{merchant_id}" style="background: #28a745; color: white; 
                   padding: 8px 12px; text-decoration: none; border-radius: 4px; margin: 2px;">Sync Now</a>
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
        <p><strong>Auto-sync:</strong> Every {SYNC_INTERVAL_HOURS} hours (checks for updates older than {SYNC_THRESHOLD_DAYS} day)</p>
        <p><strong>Next check:</strong> Background sync running</p>
    </div>
    
    <table>
        <tr>
            <th>Business Name</th>
            <th>Merchant ID</th>
            <th>Records</th>
            <th>Locations</th>
            <th>Last Sync</th>
            <th>Actions</th>
        </tr>
        {table_rows}
    </table>
    
    <div style="margin-top: 20px;">
        <a href="/signin" style="background: #28a745; color: white; padding: 10px 20px; 
           text-decoration: none; border-radius: 5px; margin: 5px;">➕ Connect New Account</a>
        <a href="/api/force-sync-all" style="background: #ffc107; color: black; padding: 10px 20px; 
           text-decoration: none; border-radius: 5px; margin: 5px;">🔄 Sync All Now</a>
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