from flask import Flask, redirect, request, jsonify, Response
import requests
import os
import json
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import threading
import time

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
        self._ensure_data_directory()
    
    def _ensure_data_directory(self):
        """Ensure data directory exists for JSON storage"""
        data_dir = 'data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            print(f"📁 Created data directory: {data_dir}")
    
    def _save_json_data(self, merchant_id, data_type, data):
        """Save JSON data to file"""
        filename = f"data/{data_type}_{merchant_id}.json"
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"💾 Saved {data_type} data to {filename}")
            return True
        except Exception as e:
            print(f"❌ Failed to save {data_type} data: {e}")
            return False
    
    def _load_json_data(self, merchant_id, data_type):
        """Load JSON data from file"""
        filename = f"data/{data_type}_{merchant_id}.json"
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
            return data
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"❌ Failed to load {data_type} data: {e}")
            return None
    
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
    
    def update_sync_status(self, merchant_id):
        """Update last sync time only"""
        sheet = self._get_sheet('tokens', create_if_missing=False)
        if not sheet:
            return False
        
        records = sheet.get_all_records()
        for i, record in enumerate(records, start=2):
            if record.get('merchant_id') == merchant_id:
                current_time = datetime.now().isoformat()
                sheet.update(f'G{i}', [[current_time]])
                print(f"✅ Updated sync status for {merchant_id}")
                return True
        
        return False
    
    def sync_merchant(self, merchant_id):
        """Complete sync process for one merchant - save to JSON files"""
        print(f"🚀 Starting sync for {merchant_id}")
        
        # Get tokens
        tokens = self.get_tokens(merchant_id)
        if not tokens:
            print(f"❌ No tokens found for {merchant_id}")
            return False
        
        access_token = tokens['access_token']
        
        # Search customers
        customers_data = self.search_customers(merchant_id, access_token)
        if customers_data:
            self._save_json_data(merchant_id, 'customers', customers_data)
        
        # Search orders
        orders_data = self.search_orders(merchant_id, access_token)
        if orders_data:
            self._save_json_data(merchant_id, 'orders', orders_data)
        
        # Search invoices
        invoices_data = self.search_invoices(merchant_id, access_token)
        if invoices_data:
            self._save_json_data(merchant_id, 'invoices', invoices_data)
        
        # Update sync status (only update last_sync, remove total_customers)
        self.update_sync_status(merchant_id)
        
        print(f"✅ Sync complete for {merchant_id}")
        return True

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
    
    def search_customers(self, merchant_id, access_token):
        """Search customers using Square API with new format"""
        print(f"👥 Searching customers for {merchant_id}")
        
        search_data = {
            "limit": 100,
            "sort": {"field": "CREATED_AT", "order": "DESC"},
            "count": True
        }
        
        response = self._make_square_request('v2/customers/search', access_token, 'POST', search_data)
        
        if not response or response.status_code != 200:
            print(f"❌ Customer search failed: {response.status_code if response else 'No response'}")
            return None
        
        data = response.json()
        print(f"✅ Customer search complete: {len(data.get('customers', []))} customers found")
        return data
    
    def search_orders(self, merchant_id, access_token):
        """Search orders using Square API with new format"""
        print(f"📦 Searching orders for {merchant_id}")
        
        # Get location IDs for this merchant
        tokens = self.get_tokens(merchant_id)
        location_ids = []
        if tokens and tokens.get('location_ids'):
            location_ids = [l.strip() for l in tokens['location_ids'].split(',') if l.strip()]
        
        if not location_ids:
            print("❌ No location IDs found for orders search")
            return None
        
        search_data = {
            "limit": 100,
            "sort": {"field": "CREATED_AT", "order": "DESC"},
            "return_entries": False,
            "filter": {"location_ids": location_ids}
        }
        
        response = self._make_square_request('v2/orders/search', access_token, 'POST', search_data)
        
        if not response or response.status_code != 200:
            print(f"❌ Orders search failed: {response.status_code if response else 'No response'}")
            return None
        
        data = response.json()
        print(f"✅ Orders search complete: {len(data.get('orders', []))} orders found")
        return data
    
    def search_invoices(self, merchant_id, access_token):
        """Search invoices using Square API with new format"""
        print(f"📄 Searching invoices for {merchant_id}")
        
        # Get location IDs for this merchant
        tokens = self.get_tokens(merchant_id)
        location_ids = []
        if tokens and tokens.get('location_ids'):
            location_ids = [l.strip() for l in tokens['location_ids'].split(',') if l.strip()]
        
        if not location_ids:
            print("❌ No location IDs found for invoices search")
            return None
        
        search_data = {
            "limit": 100,
            "sort": {"field": "INVOICE_SORT_DATE", "order": "DESC"},
            "query": {
                "filter": {
                    "location_ids": location_ids
                }
            }
        }
        
        response = self._make_square_request('v2/invoices/search', access_token, 'POST', search_data)
        
        if not response or response.status_code != 200:
            print(f"❌ Invoices search failed: {response.status_code if response else 'No response'}")
            return None
        
        data = response.json()
        print(f"✅ Invoices search complete: {len(data.get('invoices', []))} invoices found")
        return data

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
    <p>Automatically sync customer data from Square to JSON files.</p>
    <a href="/signin" class="btn">Connect Your Square Account</a>
    <a href="/dashboard" class="btn btn-success">View Dashboard</a>
    '''

@app.route('/customers')
@app.route('/customers/<merchant_id>')
def view_customers(merchant_id=None):
    """View customers JSON data"""
    if not merchant_id:
        merchants = sync.get_all_merchants()
        if len(merchants) == 1:
            merchant_id = merchants[0]['merchant_id']
        else:
            merchant_list = "<br>".join([f'<a href="/customers/{m["merchant_id"]}">{m.get("merchant_name", "Unknown")} ({m["merchant_id"]})</a>' for m in merchants])
            return f'''
            <h1>Select Merchant for Customers Data</h1>
            <div style="margin: 20px;">
                {merchant_list}
            </div>
            <a href="/dashboard">← Back to Dashboard</a>
            '''
    
    customers_data = sync._load_json_data(merchant_id, 'customers')
    if customers_data is None:
        return f'''
        <h1>No Customers Data</h1>
        <p>No customers data found for merchant {merchant_id}. Try syncing first.</p>
        <a href="/dashboard">← Back to Dashboard</a>
        ''', 404
    
    return f'''
    <h1>Customers Data - {merchant_id}</h1>
    <div style="margin: 20px 0;">
        <a href="/dashboard">← Back to Dashboard</a> | 
        <a href="/orders/{merchant_id}">Orders</a> | 
        <a href="/invoices/{merchant_id}">Invoices</a>
    </div>
    <pre style="background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: left; overflow-x: auto;">
{json.dumps(customers_data, indent=2)}
    </pre>
    '''

@app.route('/orders')
@app.route('/orders/<merchant_id>')
def view_orders(merchant_id=None):
    """View orders JSON data"""
    if not merchant_id:
        merchants = sync.get_all_merchants()
        if len(merchants) == 1:
            merchant_id = merchants[0]['merchant_id']
        else:
            merchant_list = "<br>".join([f'<a href="/orders/{m["merchant_id"]}">{m.get("merchant_name", "Unknown")} ({m["merchant_id"]})</a>' for m in merchants])
            return f'''
            <h1>Select Merchant for Orders Data</h1>
            <div style="margin: 20px;">
                {merchant_list}
            </div>
            <a href="/dashboard">← Back to Dashboard</a>
            '''
    
    orders_data = sync._load_json_data(merchant_id, 'orders')
    if orders_data is None:
        return f'''
        <h1>No Orders Data</h1>
        <p>No orders data found for merchant {merchant_id}. Try syncing first.</p>
        <a href="/dashboard">← Back to Dashboard</a>
        ''', 404
    
    return f'''
    <h1>Orders Data - {merchant_id}</h1>
    <div style="margin: 20px 0;">
        <a href="/dashboard">← Back to Dashboard</a> | 
        <a href="/customers/{merchant_id}">Customers</a> | 
        <a href="/invoices/{merchant_id}">Invoices</a>
    </div>
    <pre style="background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: left; overflow-x: auto;">
{json.dumps(orders_data, indent=2)}
    </pre>
    '''

@app.route('/invoices')
@app.route('/invoices/<merchant_id>')
def view_invoices(merchant_id=None):
    """View invoices JSON data"""
    if not merchant_id:
        merchants = sync.get_all_merchants()
        if len(merchants) == 1:
            merchant_id = merchants[0]['merchant_id']
        else:
            merchant_list = "<br>".join([f'<a href="/invoices/{m["merchant_id"]}">{m.get("merchant_name", "Unknown")} ({m["merchant_id"]})</a>' for m in merchants])
            return f'''
            <h1>Select Merchant for Invoices Data</h1>
            <div style="margin: 20px;">
                {merchant_list}
            </div>
            <a href="/dashboard">← Back to Dashboard</a>
            '''
    
    invoices_data = sync._load_json_data(merchant_id, 'invoices')
    if invoices_data is None:
        return f'''
        <h1>No Invoices Data</h1>
        <p>No invoices data found for merchant {merchant_id}. Try syncing first.</p>
        <a href="/dashboard">← Back to Dashboard</a>
        ''', 404
    
    return f'''
    <h1>Invoices Data - {merchant_id}</h1>
    <div style="margin: 20px 0;">
        <a href="/dashboard">← Back to Dashboard</a> | 
        <a href="/customers/{merchant_id}">Customers</a> | 
        <a href="/orders/{merchant_id}">Orders</a>
    </div>
    <pre style="background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: left; overflow-x: auto;">
{json.dumps(invoices_data, indent=2)}
    </pre>
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
            <td>{location_count}</td>
            <td>{sync_display}</td>
            <td>
                <a href="/api/sync/{merchant_id}" style="background: #28a745; color: white; 
                   padding: 8px 12px; text-decoration: none; border-radius: 4px; margin: 2px;">Sync</a>
                <a href="/customers/{merchant_id}" style="background: #007bff; color: white; 
                   padding: 8px 12px; text-decoration: none; border-radius: 4px; margin: 2px;">Customers</a>
                <a href="/orders/{merchant_id}" style="background: #6f42c1; color: white; 
                   padding: 8px 12px; text-decoration: none; border-radius: 4px; margin: 2px;">Orders</a>
                <a href="/invoices/{merchant_id}" style="background: #fd7e14; color: white; 
                   padding: 8px 12px; text-decoration: none; border-radius: 4px; margin: 2px;">Invoices</a>
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
           text-decoration: none; border-radius: 5px; margin: 5px;">🔄 Sync All</a>
    </div>
    '''

@app.route('/api/sync/<merchant_id>')
def manual_sync(merchant_id):
    """Manual sync trigger"""
    success = sync.sync_merchant(merchant_id)
    
    if success:
        return f'''
        <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
            <h2 style="color: #28a745;">✅ Sync Complete!</h2>
            <p><strong>Data synced to JSON files</strong></p>
            <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <div style="margin: 20px 0;">
                <a href="/customers/{merchant_id}" style="background: #007bff; color: white; padding: 10px 20px; 
                   text-decoration: none; border-radius: 5px; margin: 5px;">View Customers</a>
                <a href="/orders/{merchant_id}" style="background: #6f42c1; color: white; padding: 10px 20px; 
                   text-decoration: none; border-radius: 5px; margin: 5px;">View Orders</a>
                <a href="/invoices/{merchant_id}" style="background: #fd7e14; color: white; padding: 10px 20px; 
                   text-decoration: none; border-radius: 5px; margin: 5px;">View Invoices</a>
            </div>
            <a href="/dashboard" style="background: #6c757d; color: white; padding: 12px 24px; 
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