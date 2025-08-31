import json
import csv
from io import StringIO
from datetime import datetime, timedelta
import time

class SquareFieldExplorer:
    def __init__(self, square_sync_instance):
        """Initialize with existing SquareSync instance for token management"""
        self.sync = square_sync_instance
        self.discovered_fields = {}
        
    def explore_merchant_data(self, merchant_id, sample_size=50):
        """Main method to discover all available fields for a merchant"""
        print(f"🔍 Starting field discovery for merchant {merchant_id}")
        
        # Get merchant tokens
        tokens = self.sync.get_tokens(merchant_id)
        if not tokens or 'access_token' not in tokens:
            raise ValueError(f"No valid tokens found for merchant {merchant_id}")
        
        access_token = tokens['access_token']
        
        # Initialize field storage
        self.discovered_fields = {
            'merchant_id': merchant_id,
            'discovery_timestamp': datetime.now().isoformat(),
            'orders_fields': {},
            'catalog_fields': {},
            'field_relationships': {},
            'sample_data_counts': {}
        }
        
        # Discover Orders API fields
        print("📦 Discovering Orders API fields...")
        orders_data = self._discover_orders_fields(access_token, sample_size)
        
        # Discover Catalog API fields  
        print("📚 Discovering Catalog API fields...")
        catalog_data = self._discover_catalog_fields(access_token, sample_size)
        
        # Document field relationships
        print("🔗 Mapping field relationships...")
        self._map_field_relationships(orders_data, catalog_data)
        
        print(f"✅ Field discovery complete! Found {len(self.discovered_fields['orders_fields'])} order fields and {len(self.discovered_fields['catalog_fields'])} catalog fields")
        
        return self.discovered_fields
    
    def _discover_orders_fields(self, access_token, sample_size):
        """Comprehensive Orders API field discovery"""
        orders_sample = []
        
        # Get diverse order samples
        print("  🔄 Fetching order samples...")
        
        # Sample 1: Recent orders
        recent_orders = self._fetch_orders_sample(access_token, 'recent', sample_size // 3)
        orders_sample.extend(recent_orders)
        
        # Sample 2: Completed orders
        completed_orders = self._fetch_orders_sample(access_token, 'completed', sample_size // 3)
        orders_sample.extend(completed_orders)
        
        # Sample 3: Orders with different states
        varied_orders = self._fetch_orders_sample(access_token, 'varied', sample_size // 3)
        orders_sample.extend(varied_orders)
        
        print(f"  📊 Analyzing {len(orders_sample)} orders for field patterns...")
        
        # Extract all fields from orders
        for order in orders_sample:
            self._extract_all_fields(order, 'order', 'orders_fields')
        
        return orders_sample
    
    def _discover_catalog_fields(self, access_token, sample_size):
        """Comprehensive Catalog API field discovery"""
        catalog_sample = []
        
        print("  🔄 Fetching catalog samples...")
        
        # Get comprehensive catalog data
        all_catalog_objects = self._fetch_catalog_sample(access_token, sample_size)
        catalog_sample.extend(all_catalog_objects)
        
        print(f"  📊 Analyzing {len(catalog_sample)} catalog objects for field patterns...")
        
        # Extract fields by object type
        for catalog_obj in catalog_sample:
            obj_type = catalog_obj.get('type', 'unknown')
            path_prefix = f'catalog_{obj_type.lower()}'
            self._extract_all_fields(catalog_obj, path_prefix, 'catalog_fields')
        
        return catalog_sample
    
    def _fetch_orders_sample(self, access_token, sample_type, limit):
        """Fetch diverse order samples - FIXED VERSION"""
        # First, get order IDs from search
        order_ids = []
        
        if sample_type == 'recent':
            # Recent orders (last 30 days)
            start_date = (datetime.now() - timedelta(days=30)).isoformat() + 'Z'
            search_data = {
                'limit': limit,
                'query': {
                    'filter': {
                        'date_time_filter': {
                            'created_at': {'start_at': start_date}
                        }
                    },
                    'sort': {'sort_field': 'CREATED_AT', 'sort_order': 'DESC'}
                }
            }
        elif sample_type == 'completed':
            # Completed orders
            search_data = {
                'limit': limit,
                'query': {
                    'filter': {
                        'state_filter': {'states': ['COMPLETED']}
                    }
                }
            }
        else:  # varied
            # Mix of different states
            search_data = {
                'limit': limit,
                'query': {
                    'filter': {
                        'state_filter': {'states': ['COMPLETED', 'OPEN', 'CANCELED']}
                    }
                }
            }
        
        # Get order IDs from search
        response = self.sync._make_square_request('v2/orders/search', access_token, 'POST', search_data)
        
        if response and response.status_code == 200:
            data = response.json()
            search_orders = data.get('orders', [])
            order_ids = [order.get('id') for order in search_orders if order.get('id')]
        
        # Now fetch full details for each order ID
        full_orders = []
        for order_id in order_ids[:limit]:  # Respect the limit
            full_order = self._get_full_order_details(access_token, order_id)
            if full_order:
                full_orders.append(full_order)
            time.sleep(0.1)  # Rate limiting
        
        print(f"    ✅ Fetched {len(full_orders)} {sample_type} orders")
        return full_orders
    
    def _get_full_order_details(self, access_token, order_id):
        """Get complete order details including all nested objects"""
        response = self.sync._make_square_request(f'v2/orders/{order_id}', access_token)
        
        if response and response.status_code == 200:
            return response.json().get('order', {})
        return None
    
    def _fetch_catalog_sample(self, access_token, limit):
        """Fetch diverse catalog object samples"""
        catalog_objects = []
        
        # Get all catalog object types
        object_types = ['ITEM', 'ITEM_VARIATION', 'CATEGORY', 'TAX', 'DISCOUNT', 
                       'MODIFIER_LIST', 'MODIFIER', 'IMAGE']
        
        for obj_type in object_types:
            print(f"    📦 Fetching {obj_type} objects...")
            
            search_data = {
                'limit': min(limit // len(object_types), 100),
                'object_types': [obj_type],
                'include_deleted_objects': True  # Include deleted for complete field coverage
            }
            
            response = self.sync._make_square_request('v2/catalog/search', access_token, 'POST', search_data)
            
            if response and response.status_code == 200:
                data = response.json()
                objects = data.get('objects', [])
                catalog_objects.extend(objects)
                print(f"    ✅ Found {len(objects)} {obj_type} objects")
            
            time.sleep(0.2)  # Rate limiting
        
        return catalog_objects
    
    def _extract_all_fields(self, data_object, path_prefix, field_category):
        """Recursively extract all fields from a JSON object"""
        if not isinstance(data_object, dict):
            return
        
        for key, value in data_object.items():
            field_path = f"{path_prefix}.{key}"
            
            # Initialize field info if not seen before
            if field_path not in self.discovered_fields[field_category]:
                self.discovered_fields[field_category][field_path] = {
                    'data_type': self._get_data_type(value),
                    'example_values': [],
                    'frequency': 0,
                    'is_nested_object': isinstance(value, dict),
                    'is_array': isinstance(value, list),
                    'nested_fields': []
                }
            
            # Update field info
            field_info = self.discovered_fields[field_category][field_path]
            field_info['frequency'] += 1
            
            # Store example values (sanitized)
            example_value = self._sanitize_example_value(value, key)
            if example_value not in field_info['example_values'] and len(field_info['example_values']) < 3:
                field_info['example_values'].append(example_value)
            
            # Recursively process nested objects
            if isinstance(value, dict):
                self._extract_all_fields(value, field_path, field_category)
            elif isinstance(value, list) and value:
                # Process array elements
                for i, item in enumerate(value[:2]):  # Sample first 2 array items
                    if isinstance(item, dict):
                        array_path = f"{field_path}[{i}]"
                        self._extract_all_fields(item, array_path, field_category)
    
    def _get_data_type(self, value):
        """Determine the data type of a field value"""
        if value is None:
            return 'null'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            # Check for specific string patterns
            if self._is_timestamp(value):
                return 'timestamp'
            elif self._is_money_amount(value):
                return 'money_amount'
            elif self._is_id_field(value):
                return 'id'
            return 'string'
        elif isinstance(value, list):
            return f'array[{self._get_data_type(value[0]) if value else "unknown"}]'
        elif isinstance(value, dict):
            return 'object'
        else:
            return 'unknown'
    
    def _is_timestamp(self, value):
        """Check if string looks like a timestamp"""
        if not isinstance(value, str):
            return False
        return 'T' in value and 'Z' in value and len(value) > 10
    
    def _is_money_amount(self, value):
        """Check if this is likely a money amount field"""
        return isinstance(value, (int, str)) and str(value).isdigit()
    
    def _is_id_field(self, value):
        """Check if this looks like an ID field"""
        if not isinstance(value, str):
            return False
        return len(value) > 10 and (value.isalnum() or '_' in value or '-' in value)
    
    def _sanitize_example_value(self, value, key):
        """Sanitize example values to remove sensitive data"""
        # Sensitive field patterns
        sensitive_patterns = ['email', 'phone', 'name', 'address', 'card', 'customer']
        
        if any(pattern in key.lower() for pattern in sensitive_patterns):
            if isinstance(value, str):
                if '@' in value:
                    return 'user@example.com'
                elif value.isdigit():
                    return '555-0123'
                else:
                    return '[REDACTED]'
        
        # Truncate long strings
        if isinstance(value, str) and len(value) > 50:
            return f"{value[:47]}..."
        
        return value
    
    def _map_field_relationships(self, orders_data, catalog_data):
        """Map relationships between Orders and Catalog fields"""
        relationships = {}
        
        # Common relationship patterns
        relationships['order_to_catalog'] = {
            'order.line_items[].catalog_object_id': 'catalog_item.id',
            'order.line_items[].catalog_version': 'catalog_item.version',
            'order.line_items[].variation_name': 'catalog_item_variation.name'
        }
        
        relationships['money_fields'] = [
            field_path for field_path in self.discovered_fields['orders_fields'].keys()
            if 'money' in field_path.lower()
        ]
        
        relationships['timestamp_fields'] = [
            field_path for field_path in {**self.discovered_fields['orders_fields'], 
                                        **self.discovered_fields['catalog_fields']}.keys()
            if self.discovered_fields['orders_fields'].get(field_path, {}).get('data_type') == 'timestamp' or
               self.discovered_fields['catalog_fields'].get(field_path, {}).get('data_type') == 'timestamp'
        ]
        
        self.discovered_fields['field_relationships'] = relationships
    
    def export_field_mapping_csv(self):
        """Export comprehensive field mapping as CSV"""
        output = StringIO()
        writer = csv.writer(output)
        
        # CSV Headers
        headers = [
            'API', 'Field_Path', 'Data_Type', 'Frequency', 'Is_Nested_Object', 
            'Is_Array', 'Example_Values', 'Description'
        ]
        writer.writerow(headers)
        
        # Write Orders API fields
        for field_path, field_info in self.discovered_fields['orders_fields'].items():
            row = [
                'Orders',
                field_path,
                field_info['data_type'],
                field_info['frequency'],
                field_info['is_nested_object'],
                field_info['is_array'],
                ' | '.join(str(v) for v in field_info['example_values']),
                self._generate_field_description(field_path, field_info)
            ]
            writer.writerow(row)
        
        # Write Catalog API fields
        for field_path, field_info in self.discovered_fields['catalog_fields'].items():
            row = [
                'Catalog',
                field_path,
                field_info['data_type'],
                field_info['frequency'],
                field_info['is_nested_object'],
                field_info['is_array'],
                ' | '.join(str(v) for v in field_info['example_values']),
                self._generate_field_description(field_path, field_info)
            ]
            writer.writerow(row)
        
        # Add relationship information as separate section
        writer.writerow([])  # Empty row
        writer.writerow(['=== FIELD RELATIONSHIPS ==='])
        writer.writerow(['Relationship_Type', 'Field_1', 'Field_2', 'Description'])
        
        relationships = self.discovered_fields['field_relationships']
        
        # Order to Catalog relationships
        for order_field, catalog_field in relationships.get('order_to_catalog', {}).items():
            writer.writerow(['order_to_catalog', order_field, catalog_field, 'Links order items to catalog objects'])
        
        # Money fields grouping
        writer.writerow([])
        writer.writerow(['=== MONEY FIELDS ==='])
        for money_field in relationships.get('money_fields', []):
            writer.writerow(['money_field', money_field, '', 'Monetary value (amount in cents)'])
        
        # Timestamp fields grouping
        writer.writerow([])
        writer.writerow(['=== TIMESTAMP FIELDS ==='])
        for timestamp_field in relationships.get('timestamp_fields', []):
            writer.writerow(['timestamp_field', timestamp_field, '', 'ISO 8601 timestamp'])
        
        return output.getvalue()
    
    def _generate_field_description(self, field_path, field_info):
        """Generate human-readable description for a field"""
        path_parts = field_path.split('.')
        field_name = path_parts[-1]
        
        # Common field descriptions
        descriptions = {
            'id': 'Unique identifier',
            'created_at': 'Creation timestamp',
            'updated_at': 'Last modification timestamp',
            'version': 'Version number for optimistic concurrency',
            'state': 'Current state/status',
            'total_money': 'Total monetary amount',
            'amount': 'Amount in cents',
            'currency': 'Currency code (e.g., USD)',
            'name': 'Display name',
            'description': 'Text description',
            'quantity': 'Item quantity',
            'customer_id': 'Associated customer identifier',
            'location_id': 'Square location identifier',
            'catalog_object_id': 'Reference to catalog item',
            'line_items': 'Array of ordered items',
            'fulfillments': 'Order fulfillment information',
            'taxes': 'Applied tax information',
            'discounts': 'Applied discount information',
            'modifiers': 'Item modifiers and customizations',
            'metadata': 'Custom merchant-defined data'
        }
        
        # Check for exact match
        if field_name in descriptions:
            return descriptions[field_name]
        
        # Check for partial matches
        for pattern, desc in descriptions.items():
            if pattern in field_name.lower():
                return desc
        
        # Generate description based on data type and context
        if field_info['data_type'] == 'timestamp':
            return 'Date/time value'
        elif field_info['data_type'] == 'money_amount':
            return 'Monetary amount in cents'
        elif field_info['is_array']:
            return 'Array/list of values'
        elif field_info['is_nested_object']:
            return 'Complex object with nested fields'
        
        return 'Data field'
    
    def _fetch_catalog_sample(self, access_token, limit):
        """Fetch comprehensive catalog samples"""
        all_objects = []
        
        # Define object types to sample
        object_types = ['ITEM', 'ITEM_VARIATION', 'CATEGORY', 'TAX', 'DISCOUNT', 
                       'MODIFIER_LIST', 'MODIFIER', 'IMAGE']
        
        for obj_type in object_types:
            print(f"    📦 Sampling {obj_type} objects...")
            
            # Search for this object type
            search_data = {
                'limit': min(limit // len(object_types), 50),
                'object_types': [obj_type],
                'include_deleted_objects': True,
                'include_related_objects': True  # Get related objects for complete field coverage
            }
            
            response = self.sync._make_square_request('v2/catalog/search', access_token, 'POST', search_data)
            
            if response and response.status_code == 200:
                data = response.json()
                objects = data.get('objects', [])
                related_objects = data.get('related_objects', [])
                
                all_objects.extend(objects)
                all_objects.extend(related_objects)
                
                print(f"    ✅ Found {len(objects)} {obj_type} objects (+{len(related_objects)} related)")
            
            time.sleep(0.2)  # Rate limiting
        
        return all_objects
    
    def get_field_summary(self):
        """Get summary statistics of discovered fields"""
        if not self.discovered_fields:
            return {}
        
        orders_count = len(self.discovered_fields['orders_fields'])
        catalog_count = len(self.discovered_fields['catalog_fields'])
        
        # Analyze field types
        data_type_counts = {}
        for fields in [self.discovered_fields['orders_fields'], self.discovered_fields['catalog_fields']]:
            for field_info in fields.values():
                data_type = field_info['data_type']
                data_type_counts[data_type] = data_type_counts.get(data_type, 0) + 1
        
        return {
            'total_orders_fields': orders_count,
            'total_catalog_fields': catalog_count,
            'total_fields': orders_count + catalog_count,
            'data_type_distribution': data_type_counts,
            'discovery_timestamp': self.discovered_fields.get('discovery_timestamp'),
            'merchant_id': self.discovered_fields.get('merchant_id')
        }
    
    def generate_integration_guide(self):
        """Generate a quick integration guide based on discovered fields"""
        guide = []
        
        # Most common order fields
        order_fields = self.discovered_fields.get('orders_fields', {})
        common_order_fields = sorted(
            [(path, info) for path, info in order_fields.items() if info['frequency'] > 1],
            key=lambda x: x[1]['frequency'],
            reverse=True
        )[:10]
        
        guide.append("=== TOP 10 MOST COMMON ORDER FIELDS ===")
        for field_path, field_info in common_order_fields:
            guide.append(f"{field_path} ({field_info['data_type']}) - appears {field_info['frequency']} times")
        
        # Money fields (important for financial data)
        money_fields = [path for path in order_fields.keys() if 'money' in path.lower()]
        guide.append(f"\n=== MONEY FIELDS ({len(money_fields)} found) ===")
        for field in money_fields[:10]:
            guide.append(f"{field}")
        
        return '\n'.join(guide)