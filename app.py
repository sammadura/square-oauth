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

# Google Sheets setup
def get_google_sheets_client():
    """Initialize Google Sheets client using service account credentials"""
    try:
        creds_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
        if not creds_json:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON environment variable not found")
        
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        return gspread.authorize(creds)
    except Exception as e:
        print(f"Error initializing Google Sheets: {e}")
        return None

def save_tokens_to_sheets(merchant_id, access_token, refresh_token, merchant_name=None):
    """Save tokens to Google Sheets with proper error handling and duplicate prevention"""
    try:
        gc = get_google_sheets_client()
        if not gc:
            print("❌ Could not connect to Google Sheets")
            return False
        
        spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
        
        # Ensure the tokens sheet exists
        try:
            spreadsheet = gc.open_by_key(spreadsheet_id)
            try:
                sheet = spreadsheet.worksheet('tokens')
            except:
                print("📝 Creating 'tokens' sheet...")
                sheet = spreadsheet.add_worksheet(title='tokens', rows=1000, cols=8)
                # Add headers
                headers = ['merchant_id', 'access_token', 'refresh_token', 'updated_at', 
                          'status', 'merchant_name', 'last_sync', 'total_customers']
                sheet.append_row(headers)
        except Exception as e:
            print(f"❌ Error accessing spreadsheet: {e}")
            return False
        
        # Get all records and check for existing merchant
        try:
            records = sheet.get_all_records()
            print(f"📊 Found {len(records)} existing records in tokens sheet")
            
            # Look for existing merchant
            merchant_found = False
            for i, record in enumerate(records, start=2):  # Start at row 2 (after header)
                if record.get('merchant_id') == merchant_id:
                    print(f"🔄 Updating existing merchant {merchant_id} at row {i}")
                    current_time = datetime.now().isoformat()
                    
                    # Update existing record - make sure all fields are updated
                    update_values = [[
                        access_token, 
                        refresh_token, 
                        current_time,  # updated_at
                        'active',      # status
                        merchant_name or record.get('merchant_name', ''),  # merchant_name
                        record.get('last_sync', ''),  # keep existing last_sync
                        record.get('total_customers', 0)  # keep existing total_customers
                    ]]
                    
                    try:
                        sheet.update(f'B{i}:H{i}', update_values)
                        print(f"✅ Successfully updated merchant {merchant_id}")
                        merchant_found = True
                        break  # Important: break after first match
                    except Exception as update_error:
                        print(f"❌ Error updating row {i}: {update_error}")
                        continue  # Try next record if this update fails
            
            # Only add new record if merchant was not found AND updated
            if not merchant_found:
                print(f"➕ Adding new merchant {merchant_id}")
                current_time = datetime.now().isoformat()
                new_row = [
                    merchant_id,
                    access_token,
                    refresh_token,
                    current_time,      # updated_at
                    'active',          # status
                    merchant_name or '',  # merchant_name
                    '',                # last_sync (empty initially)
                    0                  # total_customers (0 initially)
                ]
                
                sheet.append_row(new_row)
                print(f"✅ Added new merchant {merchant_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing records: {e}")
            return False
        
    except Exception as e:
        print(f"❌ Error saving to sheets: {e}")
        return False

def get_tokens_from_sheets(merchant_id):
    """Retrieve tokens from Google Sheets"""
    try:
        gc = get_google_sheets_client()
        if not gc:
            return None
        
        spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
        sheet = gc.open_by_key(spreadsheet_id).worksheet('tokens')
        
        records = sheet.get_all_records()
        for record in records:
            if record.get('merchant_id') == merchant_id and record.get('status') == 'active':
                return {
                    'access_token': record.get('access_token'),
                    'refresh_token': record.get('refresh_token'),
                    'updated_at': record.get('updated_at'),
                    'merchant_name': record.get('merchant_name'),
                    'last_sync': record.get('last_sync'),
                    'total_customers': record.get('total_customers', 0)
                }
        return None
    except Exception as e:
        print(f"Error reading from sheets: {e}")
        return None

def get_all_active_merchants():
    """Get all active merchants for syncing"""
    try:
        gc = get_google_sheets_client()
        if not gc:
            return []
        
        spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
        sheet = gc.open_by_key(spreadsheet_id).worksheet('tokens')
        
        records = sheet.get_all_records()
        active_merchants = []
        for record in records:
            if record.get('status') == 'active':
                active_merchants.append({
                    'merchant_id': record.get('merchant_id'),
                    'merchant_name': record.get('merchant_name'),
                    'last_sync': record.get('last_sync'),
                    'total_customers': record.get('total_customers', 0)
                })
        return active_merchants
    except Exception as e:
        print(f"Error getting active merchants: {e}")
        return []

def update_sync_status(merchant_id, total_customers):
    """Update last sync time and customer count"""
    try:
        gc = get_google_sheets_client()
        if not gc:
            print("❌ Could not connect to Google Sheets for sync update")
            return False
        
        spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
        sheet = gc.open_by_key(spreadsheet_id).worksheet('tokens')
        
        records = sheet.get_all_records()
        for i, record in enumerate(records, start=2):
            if record.get('merchant_id') == merchant_id:
                current_time = datetime.now().isoformat()
                # Update last_sync (column G) and total_customers (column H)
                sheet.update(f'G{i}:H{i}', [[current_time, total_customers]])
                print(f"✅ Updated sync status for {merchant_id}: {total_customers} customers")
                return True
        
        print(f"❌ Merchant {merchant_id} not found for sync update")
        return False
    except Exception as e:
        print(f"❌ Error updating sync status: {e}")
        return False

def save_customer_data(merchant_id, customers):
    """Save customer data to a separate sheet"""
    try:
        gc = get_google_sheets_client()
        if not gc:
            return False
        
        spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
        
        # Create or get customer data sheet for this merchant
        sheet_name = f"customers_{merchant_id}"
        try:
            sheet = gc.open_by_key(spreadsheet_id).worksheet(sheet_name)
            sheet.clear()  # Clear existing data
        except:
            spreadsheet = gc.open_by_key(spreadsheet_id)
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
        
        # Headers
        headers = [
            'customer_id', 'given_name', 'family_name', 'company_name', 'nickname',
            'email_address', 'phone_number', 'address_line_1', 'address_line_2', 
            'locality', 'administrative_district_level_1', 'postal_code', 'country',
            'created_at', 'updated_at', 'birthday', 'note', 'reference_id',
            'group_ids', 'segment_ids', 'preferences', 'version', 'sync_date'
        ]
        
        # Prepare data rows
        rows = [headers]
        for customer in customers:
            row = [
                customer.get('id', ''),
                customer.get('given_name', ''),
                customer.get('family_name', ''),
                customer.get('company_name', ''),
                customer.get('nickname', ''),
                customer.get('email_address', ''),
                customer.get('phone_number', ''),
                customer.get('address', {}).get('address_line_1', ''),
                customer.get('address', {}).get('address_line_2', ''),
                customer.get('address', {}).get('locality', ''),
                customer.get('address', {}).get('administrative_district_level_1', ''),
                customer.get('address', {}).get('postal_code', ''),
                customer.get('address', {}).get('country', ''),
                customer.get('created_at', ''),
                customer.get('updated_at', ''),
                customer.get('birthday', ''),
                customer.get('note', ''),
                customer.get('reference_id', ''),
                ', '.join(customer.get('group_ids', [])),
                ', '.join(customer.get('segment_ids', [])),
                json.dumps(customer.get('preferences', {})) if customer.get('preferences') else '',
                str(customer.get('version', '')),
                datetime.now().isoformat()
            ]
            rows.append(row)
        
        # Batch update for better performance
        if len(rows) > 1:
            sheet.update(f'A1:W{len(rows)}', rows)
            
        return True
    except Exception as e:
        print(f"Error saving customer data: {e}")
        return False

def fetch_all_customers(merchant_id, access_token, use_production=False, days_back=365):
    """Fetch customers for a merchant from the last specified number of days"""
    base_url = 'https://connect.squareup.com' if use_production else 'https://connect.squareupsandbox.com'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Square-Version': '2023-10-18'
    }
    
    # Calculate date filter (last year)
    cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat() + 'Z'
    
    # Use search endpoint to filter by date
    search_data = {
        'limit': 100,
        'query': {
            'filter': {
                'created_at': {
                    'start_at': cutoff_date
                }
            }
        }
    }
    
    all_customers = []
    cursor = None
    
    while True:
        if cursor:
            search_data['cursor'] = cursor
        
        try:
            response = requests.post(f'{base_url}/v2/customers/search', headers=headers, json=search_data)
            
            if response.status_code == 200:
                data = response.json()
                customers = data.get('customers', [])
                
                # Additional client-side filtering for updated_at
                filtered_customers = []
                for customer in customers:
                    created_at = customer.get('created_at')
                    updated_at = customer.get('updated_at')
                    
                    # Include if created in last year OR updated in last year
                    include_customer = False
                    
                    if created_at:
                        try:
                            created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            if (datetime.now() - created_date.replace(tzinfo=None)).days <= days_back:
                                include_customer = True
                        except:
                            pass
                    
                    if not include_customer and updated_at:
                        try:
                            updated_date = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                            if (datetime.now() - updated_date.replace(tzinfo=None)).days <= days_back:
                                include_customer = True
                        except:
                            pass
                    
                    if include_customer:
                        filtered_customers.append(customer)
                
                all_customers.extend(filtered_customers)
                
                cursor = data.get('cursor')
                if not cursor:
                    break
            else:
                print(f"Error searching customers for {merchant_id}: {response.status_code} - {response.text}")
                # Fallback to regular endpoint if search fails
                return fetch_customers_fallback(merchant_id, access_token, use_production, days_back)
                
        except Exception as e:
            print(f"Request error for {merchant_id}: {e}")
            break
    
    print(f"Fetched {len(all_customers)} customers from last {days_back} days for {merchant_id}")
    return all_customers

def fetch_customers_fallback(merchant_id, access_token, use_production=False, days_back=365):
    """Fallback method using regular customers endpoint with client-side filtering"""
    base_url = 'https://connect.squareup.com' if use_production else 'https://connect.squareupsandbox.com'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Square-Version': '2023-10-18'
    }
    
    all_customers = []
    cursor = None
    cutoff_date = datetime.now() - timedelta(days=days_back)
    
    while True:
        params = {'limit': 100}
        if cursor:
            params['cursor'] = cursor
            
        try:
            response = requests.get(f'{base_url}/v2/customers', headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                customers = data.get('customers', [])
                
                # Filter customers by date
                filtered_customers = []
                for customer in customers:
                    created_at = customer.get('created_at')
                    updated_at = customer.get('updated_at')
                    
                    # Include if created or updated in the last year
                    include_customer = False
                    
                    if created_at:
                        try:
                            created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            if created_date.replace(tzinfo=None) >= cutoff_date:
                                include_customer = True
                        except:
                            pass
                    
                    if not include_customer and updated_at:
                        try:
                            updated_date = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                            if updated_date.replace(tzinfo=None) >= cutoff_date:
                                include_customer = True
                        except:
                            pass
                    
                    if include_customer:
                        filtered_customers.append(customer)
                
                all_customers.extend(filtered_customers)
                
                cursor = data.get('cursor')
                if not cursor:
                    break
            else:
                print(f"Error fetching customers for {merchant_id}: {response.status_code} - {response.text}")
                break
                
        except Exception as e:
            print(f"Request error for {merchant_id}: {e}")
            break
    
    print(f"Fetched {len(all_customers)} customers from last {days_back} days for {merchant_id} (fallback)")
    return all_customers

def sync_merchant_customers(merchant_id, days_back=365):
    """Sync customers for a specific merchant (last year only by default)"""
    print(f"Starting customer sync for merchant {merchant_id} (last {days_back} days)")
    
    tokens = get_tokens_from_sheets(merchant_id)
    if not tokens:
        print(f"No tokens found for merchant {merchant_id}")
        return False
    
    customers = fetch_all_customers(merchant_id, tokens['access_token'], days_back=days_back)
    
    if customers:
        success = save_customer_data(merchant_id, customers)
        if success:
            update_sync_status(merchant_id, len(customers))
            print(f"Successfully synced {len(customers)} customers for {merchant_id}")
            return True
    else:
        # Update sync status even if no customers found
        update_sync_status(merchant_id, 0)
        print(f"No customers found for {merchant_id} (synced 0 customers)")
        return True
    
    print(f"Failed to sync customers for {merchant_id}")
    return False

def should_sync_merchant(last_sync, threshold_days=3):
    """Check if merchant needs syncing"""
    if not last_sync:
        return True
    
    try:
        last_sync_date = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
        days_since_sync = (datetime.now() - last_sync_date.replace(tzinfo=None)).days
        return days_since_sync >= threshold_days
    except:
        return True

def background_sync():
    """Background task to sync all merchants"""
    sync_interval_hours = 12
    sync_threshold_days = 3
    
    print(f"🚀 Background sync started - will run every {sync_interval_hours} hours")
    
    while True:
        try:
            merchants = get_all_active_merchants()
            print(f"📊 Found {len(merchants)} active merchants to check")
            
            for merchant in merchants:
                merchant_id = merchant['merchant_id']
                last_sync = merchant.get('last_sync')
                
                if should_sync_merchant(last_sync, sync_threshold_days):
                    print(f"🔄 Syncing {merchant_id}...")
                    sync_merchant_customers(merchant_id, days_back=365)
                    time.sleep(10)  # Small delay between merchants
            
            print(f"💤 Sleeping for {sync_interval_hours} hours...")
            time.sleep(sync_interval_hours * 60 * 60)
            
        except Exception as e:
            print(f"❌ Background sync error: {e}")
            time.sleep(60 * 60)  # Sleep for 1 hour on error

@app.route('/')
def home():
    return '''
    <style>
        body { font-family: Arial, sans-serif; margin: 50px; text-align: center; }
        .btn { background: #007bff; color: white; padding: 15px 30px; text-decoration: none; border-radius: 8px; margin: 10px; display: inline-block; }
        .btn:hover { background: #0056b3; }
        .btn-success { background: #28a745; }
    </style>
    <h1>🔄 Square Customer Data Sync</h1>
    <p>Automatically sync customer data from Square to Google Sheets every few days.</p>
    <a href="/signin" class="btn">Connect Your Square Account</a>
    <a href="/dashboard" class="btn btn-success">View Dashboard</a>
    '''

@app.route('/signin')
def signin():
    client_id = os.environ.get('SQUARE_CLIENT_ID')
    if not client_id:
        return 'Error: SQUARE_CLIENT_ID not configured', 500
    
    base_url = 'https://connect.squareupsandbox.com'
    redirect_uri = os.environ.get('SQUARE_REDIRECT_URI')
    if not redirect_uri:
        return 'Error: SQUARE_REDIRECT_URI not configured', 500
    
    scope = 'CUSTOMERS_READ MERCHANT_PROFILE_READ'
    
    auth_url = (f'{base_url}/oauth2/authorize'
               f'?client_id={client_id}'
               f'&redirect_uri={redirect_uri}'
               f'&scope={scope}'
               f'&response_type=code')
    
    return redirect(auth_url)

@app.route('/oauth2callback')
def oauth2callback():
    code = request.args.get('code')
    error = request.args.get('error')
    
    if error:
        return f'Authorization denied: {error}', 400
    
    if not code:
        return 'Error: No authorization code provided', 400
    
    client_id = os.environ.get('SQUARE_CLIENT_ID')
    client_secret = os.environ.get('SQUARE_CLIENT_SECRET')
    redirect_uri = os.environ.get('SQUARE_REDIRECT_URI')
    
    # Exchange code for tokens
    token_url = 'https://connect.squareupsandbox.com/oauth2/token'
    response = requests.post(token_url, data={
        'client_id': client_id,
        'client_secret': client_secret,
        'code': code,
        'grant_type': 'authorization_code',
        'redirect_uri': redirect_uri
    })
    
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data.get('access_token')
        refresh_token = token_data.get('refresh_token')
        merchant_id = token_data.get('merchant_id')
        
        # Get merchant info for display name
        merchant_name = None
        try:
            headers = {'Authorization': f'Bearer {access_token}', 'Square-Version': '2023-10-18'}
            merchant_response = requests.get('https://connect.squareupsandbox.com/v2/merchants', headers=headers)
            if merchant_response.status_code == 200:
                merchant_data = merchant_response.json()
                if 'merchant' in merchant_data:
                    merchants = merchant_data['merchant']
                    if merchants:
                        merchant_name = merchants[0].get('business_name', 'Unknown Business')
        except Exception as e:
            print(f"Could not get merchant name: {e}")
        
        # Save tokens and start initial sync
        if save_tokens_to_sheets(merchant_id, access_token, refresh_token, merchant_name):
            # Trigger initial customer sync in background
            def initial_sync():
                try:
                    sync_merchant_customers(merchant_id, days_back=365)
                except Exception as e:
                    print(f"Initial sync error: {e}")
            
            threading.Thread(target=initial_sync, daemon=True).start()
            
            return f'''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; background: white; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); font-family: Arial, sans-serif;">
                <h1 style="color: #28a745; text-align: center;">✅ Authorization Successful!</h1>
                <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <p><strong>Business:</strong> {merchant_name or 'Unknown'}</p>
                    <p><strong>Merchant ID:</strong> {merchant_id}</p>
                    <p><strong>Status:</strong> ✅ Connected and syncing</p>
                </div>
                <div style="text-align: center; margin: 30px 0;">
                    <a href="/dashboard" style="background: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; margin: 0 10px;">View Dashboard</a>
                </div>
            </div>
            '''
        else:
            return 'Authorization successful but failed to save tokens', 500
    else:
        return f'Authorization failed: {response.text}', response.status_code

@app.route('/dashboard')
def dashboard():
    """Dashboard showing connected merchants"""
    merchants = get_all_active_merchants()
    
    html = '''
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #f2f2f2; }
        .btn { background: #007bff; color: white; padding: 8px 12px; text-decoration: none; border-radius: 4px; margin: 0 5px; font-size: 12px; }
        .btn-success { background: #28a745; }
        .btn-warning { background: #ffc107; color: black; }
    </style>
    <h1>🔄 Square Customer Data Sync Dashboard</h1>
    <p><strong>Connected Merchants:</strong> ''' + str(len(merchants)) + '''</p>
    '''
    
    if not merchants:
        html += '''
        <p>No merchants connected yet.</p>
        <a href="/signin" class="btn btn-success">Connect Square Account</a>
        '''
    else:
        html += '''
        <table>
            <tr>
                <th>Business Name</th>
                <th>Merchant ID</th>
                <th>Total Customers</th>
                <th>Last Sync</th>
                <th>Actions</th>
            </tr>
        '''
        
        for merchant in merchants:
            merchant_id = merchant['merchant_id']
            merchant_name = merchant.get('merchant_name', 'Unknown')
            total_customers = merchant.get('total_customers', 0)
            last_sync = merchant.get('last_sync', 'Never')
            
            if last_sync and last_sync != 'Never':
                try:
                    sync_date = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
                    last_sync = sync_date.strftime('%Y-%m-%d %H:%M')
                except:
                    pass
            
            html += f'''
            <tr>
                <td><strong>{merchant_name}</strong></td>
                <td><code>{merchant_id}</code></td>
                <td>{total_customers:,}</td>
                <td>{last_sync}</td>
                <td>
                    <a href="/api/sync/{merchant_id}" class="btn btn-success">Sync Now</a>
                    <a href="/api/export/{merchant_id}" class="btn">Export CSV</a>
                </td>
            </tr>
            '''
        
        html += '</table>'
    
    return html

@app.route('/api/sync/<merchant_id>')
def manual_sync(merchant_id):
    """Manual sync trigger"""
    success = sync_merchant_customers(merchant_id, days_back=365)
    if success:
        return f'<h2>✅ Sync completed for {merchant_id}</h2><a href="/dashboard">Back to Dashboard</a>'
    else:
        return f'<h2>❌ Sync failed for {merchant_id}</h2><a href="/dashboard">Back to Dashboard</a>'

@app.route('/api/export/<merchant_id>')
def export_customers(merchant_id):
    """Export customer data as CSV"""
    try:
        gc = get_google_sheets_client()
        if not gc:
            return 'Error: Could not connect to Google Sheets', 500
        
        spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
        sheet_name = f"customers_{merchant_id}"
        
        sheet = gc.open_by_key(spreadsheet_id).worksheet(sheet_name)
        data = sheet.get_all_values()
        
        if not data:
            return f'No customer data found for merchant {merchant_id}', 404
        
        # Create CSV
        output = StringIO()
        writer = csv.writer(output)
        writer.writerows(data)
        csv_data = output.getvalue()
        
        # Return CSV file
        return Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=customers_{merchant_id}_{datetime.now().strftime("%Y%m%d")}.csv'}
        )
        
    except Exception as e:
        return f'Export failed: {str(e)}', 500

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    # Start background sync thread
    sync_thread = threading.Thread(target=background_sync, daemon=True)
    sync_thread.start()
    
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))