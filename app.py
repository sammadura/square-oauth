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

def cleanup_duplicate_merchants():
    """Remove duplicate merchants keeping the most recent entry"""
    try:
        gc = get_google_sheets_client()
        if not gc:
            return False
        
        spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
        sheet = gc.open_by_key(spreadsheet_id).worksheet('tokens')
        records = sheet.get_all_records()
        
        seen_merchants = {}
        rows_to_delete = []
        
        for i, record in enumerate(records, start=2):
            merchant_id = record.get('merchant_id')
            if merchant_id in seen_merchants:
                # This is a duplicate - mark for deletion
                rows_to_delete.append(i)
                print(f"🗑️ Found duplicate merchant {merchant_id} at row {i}")
            else:
                seen_merchants[merchant_id] = i
        
        # Delete duplicate rows (from bottom to top to maintain row numbers)
        for row_num in reversed(rows_to_delete):
            sheet.delete_rows(row_num)
            print(f"✅ Deleted duplicate row {row_num}")
        
        return True
    except Exception as e:
        print(f"❌ Error cleaning duplicates: {e}")
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
        seen_merchants = set()  # Track unique merchants
        
        for record in records:
            merchant_id = record.get('merchant_id')
            if record.get('status') == 'active' and merchant_id not in seen_merchants:
                seen_merchants.add(merchant_id)
                active_merchants.append({
                    'merchant_id': merchant_id,
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
        updated_count = 0
        
        for i, record in enumerate(records, start=2):
            if record.get('merchant_id') == merchant_id:
                current_time = datetime.now().isoformat()
                # Update last_sync (column G) and total_customers (column H)
                sheet.update(f'G{i}:H{i}', [[current_time, total_customers]])
                print(f"✅ Updated sync status for {merchant_id}: {total_customers} customers at row {i}")
                updated_count += 1
        
        if updated_count == 0:
            print(f"❌ Merchant {merchant_id} not found for sync update")
            return False
        elif updated_count > 1:
            print(f"⚠️ Updated {updated_count} rows for {merchant_id} - you may have duplicates")
        
        return True
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
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=30)
        
        # Headers - now including invoice fields
        headers = [
            'customer_id', 'given_name', 'family_name', 'company_name', 'nickname',
            'email_address', 'phone_number', 'address_line_1', 'address_line_2', 
            'locality', 'administrative_district_level_1', 'postal_code', 'country',
            'created_at', 'updated_at', 'birthday', 'note', 'reference_id',
            'group_ids', 'segment_ids', 'preferences', 'version', 'sync_date',
            'latest_invoice_id', 'sale_or_service_date', 'due_date', 'invoice_status', 'invoice_amount'
        ]
        
        # Prepare data rows
        rows = [headers]
        for customer in customers:
            # Get latest invoice data for this customer if available
            latest_invoice = customer.get('latest_invoice', {})
            
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
                datetime.now().isoformat(),
                # Invoice fields
                latest_invoice.get('id', ''),
                latest_invoice.get('sale_or_service_date', ''),
                latest_invoice.get('due_date', ''),
                latest_invoice.get('invoice_status', ''),
                str(latest_invoice.get('order_total', {}).get('amount', '')) if latest_invoice.get('order_total') else ''
            ]
            rows.append(row)
        
        # Batch update for better performance
        if len(rows) > 1:
            sheet.update(f'A1:Z{len(rows)}', rows)
            print(f"✅ Saved {len(rows)-1} customer records to sheet {sheet_name}")
        else:
            print(f"⚠️ No customer data to save for {merchant_id}")
            
        return True
    except Exception as e:
        print(f"Error saving customer data: {e}")
        return False

def fetch_customer_invoices(merchant_id, access_token, customer_ids, use_production=False):
    """Fetch latest invoice data for customers"""
    if not customer_ids:
        return {}
    
    base_url = 'https://connect.squareup.com' if use_production else 'https://connect.squareupsandbox.com'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Square-Version': '2023-10-18'
    }
    
    customer_invoices = {}
    
    try:
        # Search for invoices
        search_data = {
            'query': {
                'filter': {
                    'invoice_request_method': ['EMAIL', 'CHARGE_CARD_ON_FILE', 'SHARE_MANUALLY']
                },
                'sort': {
                    'field': 'INVOICE_SORT_DATE',
                    'order': 'DESC'
                }
            },
            'limit': 100
        }
        
        response = requests.post(f'{base_url}/v2/invoices/search', headers=headers, json=search_data)
        
        if response.status_code == 200:
            data = response.json()
            invoices = data.get('invoices', [])
            
            # Map invoices to customers (get latest invoice per customer)
            for invoice in invoices:
                primary_recipient = invoice.get('primary_recipient', {})
                customer_id = primary_recipient.get('customer_id')
                
                if customer_id in customer_ids and customer_id not in customer_invoices:
                    # Get invoice details
                    invoice_request = invoice.get('invoice_request_method', {})
                    payment_requests = invoice.get('payment_requests', [])
                    
                    # Extract sale_or_service_date and due_date from payment requests
                    sale_or_service_date = ''
                    due_date = ''
                    
                    if payment_requests:
                        first_payment = payment_requests[0]
                        due_date = first_payment.get('due_date', '')
                        
                        # Sale/service date might be in the order
                        if 'tipping_enabled' in first_payment:
                            sale_or_service_date = invoice.get('created_at', '')
                    
                    customer_invoices[customer_id] = {
                        'id': invoice.get('id', ''),
                        'sale_or_service_date': sale_or_service_date,
                        'due_date': due_date,
                        'invoice_status': invoice.get('status', ''),
                        'order_total': invoice.get('order', {}).get('total_money', {}),
                        'created_at': invoice.get('created_at', ''),
                        'updated_at': invoice.get('updated_at', '')
                    }
        else:
            print(f"Error fetching invoices for {merchant_id}: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"Error fetching invoices for {merchant_id}: {e}")
    
    print(f"Fetched invoice data for {len(customer_invoices)} customers")
    return customer_invoices
def fetch_all_customers(merchant_id, access_token, use_production=False, days_back=365):
    """Fetch customers for a merchant from the last specified number of days with invoice data"""
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
    
    # Now fetch invoice data for these customers
    if all_customers:
        customer_ids = [customer.get('id') for customer in all_customers if customer.get('id')]
        invoice_data = fetch_customer_invoices(merchant_id, access_token, customer_ids, use_production)
        
        # Merge invoice data with customer data
        for customer in all_customers:
            customer_id = customer.get('id')
            if customer_id in invoice_data:
                customer['latest_invoice'] = invoice_data[customer_id]
    
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
    
    if customers or customers == []:  # Handle both cases: customers found or empty list
        success = save_customer_data(merchant_id, customers)
        if success:
            update_sync_status(merchant_id, len(customers))
            print(f"Successfully synced {len(customers)} customers for {merchant_id}")
            return True
        else:
            print(f"Failed to save customer data for {merchant_id}")
            return False
    else:
        print(f"Failed to fetch customers for {merchant_id}")
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
    """Background task to sync all merchants - optimized for free tier"""
    sync_interval_hours = 12
    sync_threshold_days = 3
    
    print(f"🚀 Background sync started - will run every {sync_interval_hours} hours")
    print(f"📅 Will sync merchants not updated in {sync_threshold_days}+ days")
    
    while True:
        try:
            sync_start_time = datetime.now()
            print(f"\n⏰ Starting background sync cycle at {sync_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            merchants = get_all_active_merchants()
            print(f"📊 Found {len(merchants)} active merchants to check")
            
            synced_count = 0
            skipped_count = 0
            failed_count = 0
            
            for i, merchant in enumerate(merchants, 1):
                merchant_id = merchant['merchant_id']
                merchant_name = merchant.get('merchant_name', 'Unknown')
                last_sync = merchant.get('last_sync')
                
                print(f"\n[{i}/{len(merchants)}] Checking {merchant_name} ({merchant_id})")
                
                # Check if we need to sync
                should_sync = True
                days_since_sync = "Never"
                
                if last_sync:
                    try:
                        last_sync_date = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
                        days_since_sync = (datetime.now() - last_sync_date.replace(tzinfo=None)).days
                        should_sync = days_since_sync >= sync_threshold_days
                        
                        print(f"   Last synced: {days_since_sync} days ago")
                    except Exception as e:
                        print(f"   Error parsing last sync date: {e}")
                        should_sync = True
                else:
                    print(f"   Last synced: Never")
                
                if should_sync:
                    print(f"   ⏳ Syncing {merchant_name}...")
                    success = sync_merchant_customers(merchant_id, days_back=365)  # Last year only
                    
                    if success:
                        synced_count += 1
                        print(f"   ✅ Sync completed for {merchant_name}")
                    else:
                        failed_count += 1
                        print(f"   ❌ Sync failed for {merchant_name}")
                    
                    # Small delay between merchants to avoid rate limiting
                    time.sleep(10)
                else:
                    skipped_count += 1
                    print(f"   ⏭️  Skipped {merchant_name} (synced {days_since_sync} days ago)")
            
            # Summary
            sync_duration = datetime.now() - sync_start_time
            print(f"\n📈 Sync cycle complete in {sync_duration.total_seconds():.1f} seconds:")
            print(f"   ✅ Synced: {synced_count}")
            print(f"   ⏭️  Skipped: {skipped_count}")
            print(f"   ❌ Failed: {failed_count}")
            
            # Sleep until next sync
            sleep_hours = sync_interval_hours
            next_sync = datetime.now() + timedelta(hours=sleep_hours)
            print(f"\n💤 Sleeping for {sleep_hours} hours until {next_sync.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(sleep_hours * 60 * 60)
            
        except Exception as e:
            print(f"❌ Background sync error: {e}")
            print("😴 Sleeping for 1 hour before retrying...")
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
    <p>Automatically sync customer data from Square to Google Sheets.</p>
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
    
    # Updated scope to include invoices
    scope = 'CUSTOMERS_READ MERCHANT_PROFILE_READ INVOICES_READ'
    
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
                        print(f"📋 Retrieved merchant name: {merchant_name}")
        except Exception as e:
            print(f"⚠️ Could not get merchant name: {e}")
        
        # Save tokens first
        print(f"💾 Saving tokens for {merchant_id}")
        if save_tokens_to_sheets(merchant_id, access_token, refresh_token, merchant_name):
            print(f"✅ Tokens saved successfully for {merchant_id}")
            
            # Trigger initial customer sync in background thread
            def initial_sync():
                print(f"🚀 Starting initial customer sync for {merchant_id}")
                try:
                    success = sync_merchant_customers(merchant_id, days_back=365)
                    if success:
                        print(f"✅ Initial sync completed for {merchant_id}")
                    else:
                        print(f"❌ Initial sync failed for {merchant_id}")
                except Exception as e:
                    print(f"❌ Initial sync error for {merchant_id}: {e}")
            
            threading.Thread(target=initial_sync, daemon=True).start()
            
            return f'''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; background: white; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); font-family: Arial, sans-serif;">
                <h1 style="color: #28a745; text-align: center;">✅ Authorization Successful!</h1>
                
                <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="margin-top: 0;">Account Connected</h3>
                    <p><strong>Business Name:</strong> {merchant_name or 'Unknown'}</p>
                    <p><strong>Merchant ID:</strong> <code>{merchant_id}</code></p>
                    <p><strong>Status:</strong> ✅ Active and ready for sync</p>
                </div>
                
                <div style="background: #fff3cd; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <h4 style="margin-top: 0;">🔄 What Happens Next</h4>
                    <ul>
                        <li>Customer data sync is running in the background</li>
                        <li>Only customers from the last year will be synced</li>
                        <li>You can manually refresh anytime from the dashboard</li>
                        <li>Data will be available for analysis in Google Colab</li>
                    </ul>
                </div>
                
                <div style="text-align: center; margin: 30px 0;">
                    <a href="/dashboard" style="background: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; margin: 0 10px;">📊 View Dashboard</a>
                    <a href="/api/refresh/{merchant_id}" style="background: #28a745; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; margin: 0 10px;">🔄 Refresh & Sync Now</a>
                </div>
                
                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0; font-size: 14px;">
                    <strong>Note:</strong> If this is a test Square account, you may need to create some test customers in your Square dashboard first.
                </div>
            </div>
            '''
        else:
            return '''
            <h2>❌ Authorization Successful but Save Failed</h2>
            <p>Your Square account was authorized but we couldn't save the tokens to Google Sheets.</p>
            <p>Please check your Google Sheets configuration and try again.</p>
            <a href="/dashboard">Go to Dashboard</a>
            ''', 500
    else:
        return f'Authorization failed: {response.text}', response.status_code

@app.route('/dashboard')
def dashboard():
    """Enhanced dashboard with refresh functionality"""
    merchants = get_all_active_merchants()
    
    html = '''
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #f2f2f2; }
        .btn { 
            background: #007bff; color: white; padding: 8px 12px; 
            text-decoration: none; border-radius: 4px; margin: 0 5px; 
            font-size: 12px; display: inline-block;
        }
        .btn:hover { background: #0056b3; }
        .btn-success { background: #28a745; }
        .btn-warning { background: #ffc107; color: black; }
        .btn-danger { background: #dc3545; }
        .status-never { color: #6c757d; font-style: italic; }
        .status-recent { color: #28a745; font-weight: bold; }
        .status-old { color: #ffc107; font-weight: bold; }
    </style>
    
    <h1>🔄 Square Customer Data Sync Dashboard</h1>
    
    <div style="background: #e9ecef; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
        <h3>📊 System Status</h3>
        <p><strong>Connected Merchants:</strong> ''' + str(len(merchants)) + '''</p>
        <p><strong>Auto-sync:</strong> Every 12 hours (merchants not synced in 3+ days)</p>
        <p><strong>Data Filter:</strong> Customers from last 365 days only</p>
    </div>
    
    <h2>Connected Square Accounts</h2>
    '''
    
    if not merchants:
        html += '''
        <div style="background: #fff3cd; padding: 15px; border-radius: 8px; margin: 20px 0;">
            <h4>No merchants connected yet</h4>
            <p>Have your clients connect their Square accounts to get started.</p>
            <a href="/signin" class="btn btn-success">Connect First Account</a>
        </div>
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
            merchant_name = merchant.get('merchant_name', 'Unknown Business')
            total_customers = merchant.get('total_customers', 0)
            last_sync = merchant.get('last_sync', '')
            
            # Format last sync
            sync_display = 'Never'
            sync_class = 'status-never'
            
            if last_sync and last_sync != 'Never':
                try:
                    sync_date = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
                    days_ago = (datetime.now() - sync_date.replace(tzinfo=None)).days
                    
                    if days_ago == 0:
                        sync_display = 'Today'
                        sync_class = 'status-recent'
                    elif days_ago == 1:
                        sync_display = 'Yesterday'
                        sync_class = 'status-recent'
                    elif days_ago < 7:
                        sync_display = f'{days_ago} days ago'
                        sync_class = 'status-recent'
                    else:
                        sync_display = f'{days_ago} days ago'
                        sync_class = 'status-old'
                except:
                    sync_display = 'Unknown'
                    sync_class = 'status-never'
            
            html += f'''
            <tr>
                <td><strong>{merchant_name}</strong></td>
                <td><code>{merchant_id}</code></td>
                <td>{total_customers:,}</td>
                <td class="{sync_class}">{sync_display}</td>
                <td>
                    <a href="/api/refresh/{merchant_id}" class="btn btn-warning">🔄 Refresh Token & Sync</a>
                    <a href="/api/sync/{merchant_id}" class="btn btn-success">📥 Manual Sync</a>
                    <a href="/api/export/{merchant_id}" class="btn">📊 Export CSV</a>
                    <a href="/debug/{merchant_id}" class="btn">🔍 Debug</a>
                </td>
            </tr>
            '''
        
        html += '</table>'
    
    html += '''
    <div style="margin-top: 30px; padding: 15px; background: #f8f9fa; border-radius: 8px;">
        <h3>📋 Quick Actions</h3>
        <a href="/signin" class="btn btn-success">➕ Connect New Account</a>
        <a href="/api/force-sync-all" class="btn btn-warning">🔄 Force Sync All</a>
        <a href="/api/cleanup-duplicates" class="btn btn-danger">🗑️ Clean Duplicates</a>
        <a href="/health" class="btn">❤️ Health Check</a>
    </div>
    
    <div style="margin-top: 20px; padding: 15px; background: #e7f3ff; border-radius: 8px;">
        <h4>💡 How It Works</h4>
        <ul>
            <li><strong>🔄 Refresh Token & Sync:</strong> Updates the access token and pulls fresh customer data</li>
            <li><strong>📥 Manual Sync:</strong> Pulls customer data using existing token</li>
            <li><strong>📊 Export CSV:</strong> Download current customer data as CSV</li>
            <li><strong>🔍 Debug:</strong> Check connection status and troubleshoot issues</li>
            <li><strong>🗑️ Clean Duplicates:</strong> Remove duplicate merchant entries</li>
        </ul>
    </div>
    '''
    
    return html

@app.route('/api/sync/<merchant_id>')
def manual_sync(merchant_id):
    """Manual sync trigger - last year only"""
    print(f"📥 Manual sync requested for {merchant_id}")
    
    try:
        success = sync_merchant_customers(merchant_id, days_back=365)
        
        if success:
            # Get updated merchant info
            tokens = get_tokens_from_sheets(merchant_id)
            merchant_name = tokens.get('merchant_name', 'Unknown') if tokens else 'Unknown'
            customer_count = tokens.get('total_customers', 0) if tokens else 0
            
            return f'''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; background: white; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); font-family: Arial, sans-serif;">
                <h2 style="color: #28a745; text-align: center;">✅ Sync Completed Successfully!</h2>
                
                <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="margin-top: 0;">📊 Sync Results</h3>
                    <p><strong>Business:</strong> {merchant_name}</p>
                    <p><strong>Merchant ID:</strong> <code>{merchant_id}</code></p>
                    <p><strong>Customers Synced:</strong> {customer_count:,} (from last year)</p>
                    <p><strong>Data Saved:</strong> Google Sheets updated</p>
                </div>
                
                <div style="text-align: center; margin: 30px 0;">
                    <a href="/dashboard" style="background: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; margin: 0 10px;">📊 Back to Dashboard</a>
                    <a href="/api/export/{merchant_id}" style="background: #28a745; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; margin: 0 10px;">📥 Export CSV</a>
                </div>
                
                {f'<div style="background: #fff3cd; padding: 15px; border-radius: 8px; margin: 20px 0;"><strong>Note:</strong> No customers found. This could be a test account or all customers are older than 1 year.</div>' if customer_count == 0 else ''}
            </div>
            '''
        else:
            return f'''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; background: white; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); font-family: Arial, sans-serif;">
                <h2 style="color: #dc3545; text-align: center;">❌ Sync Failed</h2>
                
                <div style="background: #f8d7da; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="margin-top: 0;">❌ Error Details</h3>
                    <p><strong>Merchant ID:</strong> <code>{merchant_id}</code></p>
                    <p><strong>Issue:</strong> Could not sync customer data</p>
                    <p><strong>Possible causes:</strong></p>
                    <ul>
                        <li>Access token expired (try refresh)</li>
                        <li>Network connectivity issue</li>
                        <li>Square API rate limits</li>
                        <li>Google Sheets permissions</li>
                    </ul>
                </div>
                
                <div style="text-align: center; margin: 30px 0;">
                    <a href="/api/refresh/{merchant_id}" style="background: #ffc107; color: black; padding: 12px 24px; text-decoration: none; border-radius: 5px; margin: 0 10px;">🔄 Try Refresh & Sync</a>
                    <a href="/debug/{merchant_id}" style="background: #6c757d; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; margin: 0 10px;">🔍 Debug</a>
                    <a href="/dashboard" style="background: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; margin: 0 10px;">📊 Dashboard</a>
                </div>
            </div>
            '''
            
    except Exception as e:
        print(f"❌ Manual sync error for {merchant_id}: {e}")
        return f'''
        <div style="max-width: 600px; margin: 50px auto; padding: 30px; background: white; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); font-family: Arial, sans-serif;">
            <h2 style="color: #dc3545; text-align: center;">❌ Sync Error</h2>
            <div style="background: #f8d7da; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <p><strong>Error:</strong> {str(e)}</p>
            </div>
            <div style="text-align: center;">
                <a href="/dashboard" style="background: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px;">Back to Dashboard</a>
            </div>
        </div>
        ''', 500

@app.route('/api/refresh/<merchant_id>')
def refresh_and_sync(merchant_id):
    """Refresh token and sync customer data - no re-login required"""
    print(f"🔄 Token refresh and sync requested for {merchant_id}")
    
    try:
        # Get current tokens
        tokens = get_tokens_from_sheets(merchant_id)
        if not tokens:
            return f'''
            <h2>❌ Merchant Not Found</h2>
            <p>Could not find tokens for merchant {merchant_id}</p>
            <a href="/dashboard">Back to Dashboard</a>
            ''', 404
        
        refresh_token = tokens.get('refresh_token')
        if not refresh_token:
            return f'''
            <h2>❌ No Refresh Token</h2>
            <p>No refresh token found for merchant {merchant_id}. Please re-authorize.</p>
            <a href="/signin">Re-authorize Square Account</a> | 
            <a href="/dashboard">Back to Dashboard</a>
            ''', 404
        
        # Refresh the access token
        print(f"🔑 Refreshing access token for {merchant_id}")
        client_id = os.environ.get('SQUARE_CLIENT_ID')
        client_secret = os.environ.get('SQUARE_CLIENT_SECRET')
        
        token_url = 'https://connect.squareupsandbox.com/oauth2/token'
        response = requests.post(token_url, data={
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token'
        })
        
        if response.status_code == 200:
            token_data = response.json()
            new_access_token = token_data.get('access_token')
            new_refresh_token = token_data.get('refresh_token', refresh_token)  # Use old if not provided
            
            print(f"✅ Token refreshed successfully for {merchant_id}")
            
            # Update tokens in Google Sheets
            if save_tokens_to_sheets(merchant_id, new_access_token, new_refresh_token, tokens.get('merchant_name')):
                print(f"✅ Updated tokens in Google Sheets for {merchant_id}")
                
                # Now sync customer data with new token
                print(f"📥 Starting customer sync with refreshed token for {merchant_id}")
                success = sync_merchant_customers(merchant_id, days_back=365)
                
                if success:
                    tokens = get_tokens_from_sheets(merchant_id)  # Get updated info
                    customer_count = tokens.get('total_customers', 0) if tokens else 0
                    
                    return f'''
                    <h2>✅ Refresh and Sync Complete!</h2>
                    <p><strong>Merchant:</strong> {merchant_id}</p>
                    <p><strong>✅ Token refreshed:</strong> New access token obtained</p>
                    <p><strong>✅ Data synced:</strong> {customer_count} customers from last year</p>
                    <p><strong>✅ Sheets updated:</strong> Customer data saved to Google Sheets</p>
                    <br>
                    <a href="/dashboard" style="background: #28a745; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Back to Dashboard</a>
                    <a href="/api/export/{merchant_id}" style="background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Export CSV</a>
                    '''
                else:
                    # Token refresh worked but sync failed
                    return f'''
                    <h2>⚠️ Token Refreshed but Sync Failed</h2>
                    <p><strong>Merchant:</strong> {merchant_id}</p>
                    <p><strong>✅ Token refreshed:</strong> Successfully</p>
                    <p><strong>❌ Customer sync:</strong> Failed to sync customer data</p>
                    <br>
                    <a href="/debug/{merchant_id}" style="background: #ffc107; color: black; padding: 10px 20px; text-decoration: none; border-radius: 5px;">🔍 Debug</a>
                    <a href="/dashboard" style="background: #28a745; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Back to Dashboard</a>
                    '''
            else:
                return f'''
                <h2>❌ Token Refresh Failed</h2>
                <p>Token was refreshed but could not save to Google Sheets.</p>
                <a href="/debug/{merchant_id}">Debug</a> | <a href="/dashboard">Back to Dashboard</a>
                ''', 500
                
        else:
            print(f"❌ Token refresh failed for {merchant_id}: {response.status_code} - {response.text}")
            return f'''
            <h2>❌ Token Refresh Failed</h2>
            <p><strong>Error:</strong> {response.status_code} - {response.text}</p>
            <p>The refresh token may be expired. Please re-authorize your Square account.</p>
            <br>
            <a href="/signin" style="background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Re-authorize Square Account</a>
            <a href="/dashboard" style="background: #6c757d; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Back to Dashboard</a>
            ''', 400
            
    except Exception as e:
        print(f"❌ Error in refresh_and_sync for {merchant_id}: {e}")
        return f'''
        <h2>❌ Refresh Error</h2>
        <p>An error occurred while refreshing: {str(e)}</p>
        <a href="/dashboard">Back to Dashboard</a>
        ''', 500

@app.route('/api/force-sync-all')
def force_sync_all():
    """Force sync all merchants regardless of last sync time"""
    print("🚀 Force sync all merchants requested")
    
    merchants = get_all_active_merchants()
    if not merchants:
        return '''
        <h2>No Merchants Found</h2>
        <p>No connected merchants to sync.</p>
        <a href="/signin">Connect Square Account</a> | <a href="/dashboard">Back to Dashboard</a>
        '''
    
    results = []
    for merchant in merchants:
        merchant_id = merchant['merchant_id']
        merchant_name = merchant.get('merchant_name', 'Unknown')
        
        print(f"🔄 Force syncing {merchant_name} ({merchant_id})")
        success = sync_merchant_customers(merchant_id, days_back=365)
        
        if success:
            results.append(f"✅ {merchant_name}")
        else:
            results.append(f"❌ {merchant_name}")
    
    results_html = '<br>'.join(results)
    
    return f'''
    <h2>🔄 Force Sync All Complete</h2>
    <p>Attempted to sync {len(merchants)} merchants:</p>
    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0;">
        {results_html}
    </div>
    <a href="/dashboard">Back to Dashboard</a>
    '''

@app.route('/api/cleanup-duplicates')
def cleanup_duplicates_endpoint():
    """Endpoint to clean up duplicate merchants"""
    try:
        success = cleanup_duplicate_merchants()
        if success:
            return '''
            <h2>🗑️ Duplicate Cleanup Complete</h2>
            <p>Successfully removed duplicate merchant entries from Google Sheets.</p>
            <a href="/dashboard">Back to Dashboard</a>
            '''
        else:
            return '''
            <h2>❌ Cleanup Failed</h2>
            <p>Could not clean up duplicates. Check logs for details.</p>
            <a href="/dashboard">Back to Dashboard</a>
            '''
    except Exception as e:
        return f'''
        <h2>❌ Cleanup Error</h2>
        <p>Error during cleanup: {str(e)}</p>
        <a href="/dashboard">Back to Dashboard</a>
        '''

@app.route('/api/export/<merchant_id>')
def export_customers(merchant_id):
    """Export customer data as CSV"""
    try:
        gc = get_google_sheets_client()
        if not gc:
            return 'Error: Could not connect to Google Sheets', 500
        
        spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
        sheet_name = f"customers_{merchant_id}"
        
        try:
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
            return f'Customer data not found for merchant {merchant_id}. Error: {str(e)}', 404
            
    except Exception as e:
        return f'Export failed: {str(e)}', 500

@app.route('/api/merchants')
def list_merchants():
    """API endpoint to list all merchants"""
    api_key = request.headers.get('X-API-Key')
    if api_key != os.environ.get('API_KEY'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    merchants = get_all_active_merchants()
    return jsonify({'merchants': merchants})

@app.route('/api/cron-sync')
def cron_sync():
    """External cron sync endpoint"""
    # Verify authorization
    auth_token = request.headers.get('Authorization')
    expected_token = f"Bearer {os.environ.get('CRON_TOKEN')}"
    
    if auth_token != expected_token:
        return jsonify({'error': 'Unauthorized'}), 401
    
    print(f"🤖 Cron sync triggered at {datetime.now().isoformat()}")
    
    merchants = get_all_active_merchants()
    results = []
    synced_count = 0
    
    for merchant in merchants:
        merchant_id = merchant['merchant_id']
        merchant_name = merchant.get('merchant_name', 'Unknown')
        last_sync = merchant.get('last_sync')
        
        if should_sync_merchant(last_sync):
            print(f"🔄 Syncing {merchant_name} ({merchant_id})")
            success = sync_merchant_customers(merchant_id, days_back=365)
            
            if success:
                synced_count += 1
                results.append(f"✅ {merchant_name}")
            else:
                results.append(f"❌ {merchant_name}")
        else:
            results.append(f"⏭️ {merchant_name} (recently synced)")
    
    return jsonify({
        'status': 'completed',
        'synced_count': synced_count,
        'total_merchants': len(merchants),
        'results': results,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/debug/<merchant_id>')
def debug_merchant(merchant_id):
    """Debug endpoint to check merchant setup"""
    debug_info = {
        'merchant_id': merchant_id,
        'timestamp': datetime.now().isoformat(),
        'checks': {}
    }
    
    # Check 1: Can we connect to Google Sheets?
    try:
        gc = get_google_sheets_client()
        if gc:
            debug_info['checks']['google_sheets_connection'] = '✅ Connected'
            
            # Check if we can access the spreadsheet
            try:
                spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
                sheet = gc.open_by_key(spreadsheet_id)
                debug_info['checks']['spreadsheet_access'] = f'✅ Can access: {sheet.title}'
            except Exception as e:
                debug_info['checks']['spreadsheet_access'] = f'❌ Cannot access spreadsheet: {str(e)}'
        else:
            debug_info['checks']['google_sheets_connection'] = '❌ Cannot connect to Google Sheets'
    except Exception as e:
        debug_info['checks']['google_sheets_connection'] = f'❌ Error: {str(e)}'
    
    # Check 2: Do we have tokens for this merchant?
    tokens = None
    try:
        tokens = get_tokens_from_sheets(merchant_id)
        if tokens:
            debug_info['checks']['merchant_tokens'] = '✅ Tokens found'
            debug_info['merchant_name'] = tokens.get('merchant_name', 'Unknown')
            debug_info['last_sync'] = tokens.get('last_sync', 'Never')
            debug_info['total_customers'] = tokens.get('total_customers', 0)
        else:
            debug_info['checks']['merchant_tokens'] = '❌ No tokens found'
    except Exception as e:
        debug_info['checks']['merchant_tokens'] = f'❌ Error getting tokens: {str(e)}'
    
    # Check 3: Can we call Square API?
    if tokens:
        try:
            headers = {
                'Authorization': f'Bearer {tokens["access_token"]}',
                'Content-Type': 'application/json',
                'Square-Version': '2023-10-18'
            }
            
            # Test merchant info API
            response = requests.get('https://connect.squareupsandbox.com/v2/merchants', headers=headers)
            if response.status_code == 200:
                debug_info['checks']['square_api'] = '✅ Square API accessible'
                
                # Test customers API
                customer_response = requests.get('https://connect.squareupsandbox.com/v2/customers?limit=5', headers=headers)
                if customer_response.status_code == 200:
                    customer_data = customer_response.json()
                    customer_count = len(customer_data.get('customers', []))
                    debug_info['checks']['customer_data'] = f'✅ Found {customer_count} customers (showing first 5)'
                    debug_info['sample_customers'] = customer_data.get('customers', [])
                else:
                    debug_info['checks']['customer_data'] = f'❌ Customer API error: {customer_response.status_code}'
            else:
                debug_info['checks']['square_api'] = f'❌ Square API error: {response.status_code} - {response.text}'
        except Exception as e:
            debug_info['checks']['square_api'] = f'❌ Error calling Square API: {str(e)}'
    
    # Check 4: Does customer sheet exist?
    if gc:
        try:
            spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID')
            spreadsheet = gc.open_by_key(spreadsheet_id)
            sheet_name = f"customers_{merchant_id}"
            
            try:
                customer_sheet = spreadsheet.worksheet(sheet_name)
                row_count = len(customer_sheet.get_all_values())
                debug_info['checks']['customer_sheet'] = f'✅ Sheet exists with {row_count} rows'
            except:
                debug_info['checks']['customer_sheet'] = f'❌ Sheet "{sheet_name}" does not exist'
        except Exception as e:
            debug_info['checks']['customer_sheet'] = f'❌ Error checking customer sheet: {str(e)}'
    
    # Check 5: Environment variables
    env_checks = {}
    required_vars = ['GOOGLE_SHEETS_ID', 'GOOGLE_SERVICE_ACCOUNT_JSON', 'SQUARE_CLIENT_ID', 'SQUARE_CLIENT_SECRET']
    for var in required_vars:
        if os.environ.get(var):
            env_checks[var] = '✅ Set'
        else:
            env_checks[var] = '❌ Missing'
    
    debug_info['checks']['environment_variables'] = env_checks
    
    # Format as HTML for easy reading
    html = f"""
    <h1>Debug Info for Merchant {merchant_id}</h1>
    <pre>{json.dumps(debug_info, indent=2, default=str)}</pre>
    <br>
    <a href="/api/sync/{merchant_id}">🔄 Try Manual Sync</a> | 
    <a href="/dashboard">📊 Back to Dashboard</a>
    """
    
    return html

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    # Start background sync thread
    sync_thread = threading.Thread(target=background_sync, daemon=True)
    sync_thread.start()
    
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))