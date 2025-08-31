import os
import json
import requests
from datetime import datetime, timedelta

SQUARE_API_VERSION = '2023-10-18'

def _make_square_request(endpoint, access_token, method='GET', data=None):
    """Make Square API request"""
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
            response = requests.get(url, headers=headers)
        
        # Add debug logging
        print(f"📡 API Request: {method} {url}")
        print(f"📝 Response Status: {response.status_code}")
        print(f"📄 Response Body: {response.text[:200]}...")  # First 200 chars
        
        return response
    except Exception as e:
        print(f"❌ API error: {e}")
        return None

def explore_invoice_fields(access_token):
    """Fetch and explore fields from a single invoice"""
    
    # 1. First get locations
    locations_response = _make_square_request('v2/locations', access_token)
    if not locations_response or locations_response.status_code != 200:
        print("❌ Failed to get locations")
        return
    
    locations = locations_response.json().get('locations', [])
    location_ids = [loc['id'] for loc in locations if loc.get('id')]
    
    # 2. Search for invoice by customer name
    search_data = {
        'query': {
            'filter': {
                'location_ids': location_ids,
                'customer_filter': {
                    'given_name': ['Hailey'],
                    'family_name': ['Fadden']
                }
            }
        },
        'limit': 1
    }
    
    invoice_response = _make_square_request('v2/invoices/search', 
                                          access_token, 
                                          'POST', 
                                          search_data)
    
    
    if not invoice_response or invoice_response.status_code != 200:
        print("❌ Failed to get invoice")
        return
    
    invoices = invoice_response.json().get('invoices', [])
    if not invoices:
        print("❌ No invoices found")
        return
    
    invoice = invoices[0]
    
    # 3. Get associated order if exists
    order_id = invoice.get('order_id')
    order_data = None
    
    if order_id:
        order_response = _make_square_request('v2/orders/batch-retrieve',
                                            access_token,
                                            'POST',
                                            {'order_ids': [order_id]})
        
        if order_response and order_response.status_code == 200:
            orders = order_response.json().get('orders', [])
            if orders:
                order_data = orders[0]
    
    # 4. Print all fields with nice formatting
    print("\n=== 📋 INVOICE FIELDS EXPLORATION ===\n")
    
    # Basic invoice info
    print("🔷 BASIC INVOICE INFO:")
    print(f"ID: {invoice.get('id')}")
    print(f"Invoice Number: {invoice.get('invoice_number')}")
    print(f"Status: {invoice.get('status')}")
    print(f"Created At: {invoice.get('created_at')}")
    print(f"Updated At: {invoice.get('updated_at')}")
    
    # Customer info
    print("\n🔷 CUSTOMER INFO:")
    customer = invoice.get('primary_recipient', {})
    print(json.dumps(customer, indent=2))
    
    # Payment requests
    print("\n🔷 PAYMENT REQUESTS:")
    for req in invoice.get('payment_requests', []):
        print(json.dumps(req, indent=2))
    
    # Order info
    if order_data:
        print("\n🔷 ASSOCIATED ORDER INFO:")
        print(json.dumps(order_data, indent=2))
    
    # Save full response for reference
    print("\n🔷 SAVING FULL RESPONSE TO invoice_example.json")
    with open('invoice_example.json', 'w') as f:
        json.dump({
            'invoice': invoice,
            'order': order_data
        }, f, indent=2)

if __name__ == '__main__':
    # Get access token from environment
    access_token = os.environ.get('SQUARE_ACCESS_TOKEN')
    if not access_token:
        print("❌ Please set SQUARE_ACCESS_TOKEN environment variable")
        exit(1)
    
    explore_invoice_fields(access_token)