from flask import Response, request, jsonify
from datetime import datetime
import threading
import time
from square_field_explorer import SquareFieldExplorer

def add_explorer_routes(app, sync_instance):
    """Add field explorer routes to existing Flask app"""
    
    @app.route('/explorer')
    def explorer_home():
        """Main explorer interface"""
        merchants = sync_instance.get_all_merchants()
        
        if not merchants:
            return '''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
                <h1>🔍 Square API Field Explorer</h1>
                <div style="background: #fff3cd; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3>No merchants connected</h3>
                    <p>You need to connect a Square account first to explore API fields.</p>
                </div>
                <a href="/signin" style="background: #28a745; color: white; padding: 15px 30px; 
                   text-decoration: none; border-radius: 8px;">Connect Square Account</a>
            </div>
            '''
        
        # Build merchant selection
        merchant_cards = ""
        for merchant in merchants:
            merchant_id = merchant['merchant_id']
            name = merchant.get('merchant_name', 'Unknown')
            customers = merchant.get('total_customers', 0)
            last_sync = merchant.get('last_sync', 'Never')
            
            merchant_cards += f'''
            <div style="background: white; padding: 20px; border-radius: 8px; margin: 15px 0; 
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h3 style="margin: 0 0 10px 0; color: #007bff;">{name}</h3>
                <p><strong>Merchant ID:</strong> <code>{merchant_id}</code></p>
                <p><strong>Customers:</strong> {customers:,}</p>
                <p><strong>Last Sync:</strong> {last_sync}</p>
                <div style="margin-top: 15px;">
                    <a href="/explorer/discover/{merchant_id}" 
                       style="background: #007bff; color: white; padding: 10px 20px; 
                              text-decoration: none; border-radius: 5px; margin-right: 10px;">
                        🔍 Discover Fields
                    </a>
                    <a href="/explorer/quick-export/{merchant_id}" 
                       style="background: #28a745; color: white; padding: 10px 20px; 
                              text-decoration: none; border-radius: 5px;">
                        📊 Quick Export
                    </a>
                </div>
            </div>
            '''
        
        return f'''
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background: #f8f9fa; }}
            .container {{ max-width: 800px; margin: 0 auto; }}
            .btn {{ background: #007bff; color: white; padding: 10px 20px; 
                    text-decoration: none; border-radius: 5px; display: inline-block; }}
        </style>
        
        <div class="container">
            <h1>🔍 Square API Field Explorer</h1>
            
            <div style="background: #e9ecef; padding: 20px; border-radius: 8px; margin-bottom: 30px;">
                <h3>📋 What this tool does:</h3>
                <ul>
                    <li><strong>Orders API:</strong> Discovers all available fields in orders, line items, payments, fulfillments</li>
                    <li><strong>Catalog API:</strong> Maps all fields in items, variations, categories, modifiers, taxes</li>
                    <li><strong>Field Relationships:</strong> Shows how order and catalog data connect</li>
                    <li><strong>Export:</strong> Generates comprehensive CSV documentation for integration</li>
                </ul>
            </div>
            
            <h2>📊 Select Merchant to Explore</h2>
            {merchant_cards}
            
            <div style="text-align: center; margin-top: 30px;">
                <a href="/dashboard" class="btn" style="background: #6c757d;">← Back to Main Dashboard</a>
            </div>
        </div>
        '''
    
    @app.route('/explorer/discover/<merchant_id>')
    def discover_fields(merchant_id):
        """Start field discovery process"""
        try:
            # Verify merchant exists
            tokens = sync_instance.get_tokens(merchant_id)
            if not tokens:
                return f'''
                <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
                    <h2 style="color: #dc3545;">❌ Error</h2>
                    <p>Merchant {merchant_id} not found or not connected</p>
                    <a href="/explorer" style="background: #007bff; color: white; padding: 12px 24px; 
                       text-decoration: none; border-radius: 5px;">Back to Explorer</a>
                </div>
                ''', 404
            
            merchant_name = tokens.get('merchant_name', 'Unknown')
            
            return f'''
            <div style="max-width: 700px; margin: 50px auto; padding: 30px; text-align: center;">
                <h2>🔍 Field Discovery</h2>
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <p><strong>Merchant:</strong> {merchant_name}</p>
                    <p><strong>Merchant ID:</strong> {merchant_id}</p>
                </div>
                
                <div style="background: #fff3cd; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <h4>⚡ Discovery Process</h4>
                    <p>This will analyze your Square data to discover all available API fields:</p>
                    <ul style="text-align: left; display: inline-block;">
                        <li>Sample ~50 orders with different characteristics</li>
                        <li>Sample catalog items, variations, and categories</li>
                        <li>Extract all field paths and data types</li>
                        <li>Map relationships between APIs</li>
                        <li>Generate comprehensive CSV documentation</li>
                    </ul>
                    <p><em>Estimated time: 2-5 minutes depending on data volume</em></p>
                </div>
                
                <form method="POST" action="/explorer/run-discovery/{merchant_id}">
                    <div style="margin: 20px 0;">
                        <label style="display: block; margin-bottom: 10px;">
                            <strong>Sample Size:</strong>
                        </label>
                        <select name="sample_size" style="padding: 8px; border-radius: 4px;">
                            <option value="25">Small (25 samples) - Quick</option>
                            <option value="50" selected>Medium (50 samples) - Recommended</option>
                            <option value="100">Large (100 samples) - Comprehensive</option>
                        </select>
                    </div>
                    
                    <button type="submit" style="background: #28a745; color: white; padding: 15px 30px; 
                            border: none; border-radius: 8px; cursor: pointer; font-size: 16px;">
                        🚀 Start Discovery
                    </button>
                </form>
                
                <div style="margin-top: 20px;">
                    <a href="/explorer" style="color: #6c757d; text-decoration: none;">← Back to Explorer</a>
                </div>
            </div>
            '''
            
        except Exception as e:
            return f'Error: {str(e)}', 500
    
    @app.route('/explorer/run-discovery/<merchant_id>', methods=['POST'])
    def run_discovery(merchant_id):
        """Execute the field discovery process"""
        try:
            sample_size = int(request.form.get('sample_size', 50))
            
            # Create explorer instance
            explorer = SquareFieldExplorer(sync_instance)
            
            # Run discovery (this might take a few minutes)
            print(f"🔍 Starting field discovery for {merchant_id} with sample size {sample_size}")
            discovered_fields = explorer.explore_merchant_data(merchant_id, sample_size)
            
            # Get summary stats
            summary = explorer.get_field_summary()
            
            # Generate CSV export
            csv_data = explorer.export_field_mapping_csv()
            
            # Store the explorer instance temporarily (in a real app, you'd save this to database/cache)
            # For now, we'll just return the download immediately
            filename = f'square_fields_{merchant_id}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
            
            # Show results page with download link
            merchant_name = sync_instance.get_tokens(merchant_id).get('merchant_name', 'Unknown')
            
            return f'''
            <div style="max-width: 800px; margin: 50px auto; padding: 30px;">
                <h2 style="color: #28a745;">✅ Field Discovery Complete!</h2>
                
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3>📊 Discovery Results</h3>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                        <div>
                            <p><strong>Merchant:</strong> {merchant_name}</p>
                            <p><strong>Total Fields Found:</strong> {summary['total_fields']}</p>
                            <p><strong>Orders API Fields:</strong> {summary['total_orders_fields']}</p>
                            <p><strong>Catalog API Fields:</strong> {summary['total_catalog_fields']}</p>
                        </div>
                        <div>
                            <p><strong>Discovery Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                            <p><strong>Sample Size:</strong> {sample_size} objects</p>
                            <p><strong>Data Types Found:</strong> {len(summary['data_type_distribution'])}</p>
                        </div>
                    </div>
                </div>
                
                <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0; text-align: center;">
                    <h3>📁 Download Field Documentation</h3>
                    <p>Complete CSV mapping of all discovered API fields with examples and descriptions</p>
                    <form method="POST" action="/explorer/download-csv/{merchant_id}" style="display: inline;">
                        <input type="hidden" name="csv_data" value="{csv_data.replace('"', '&quot;')}">
                        <input type="hidden" name="filename" value="{filename}">
                        <button type="submit" style="background: #007bff; color: white; padding: 15px 30px; 
                                border: none; border-radius: 8px; cursor: pointer; font-size: 16px;">
                            📥 Download CSV Report
                        </button>
                    </form>
                </div>
                
                <div style="background: #fff3cd; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <h4>💡 What's in the CSV:</h4>
                    <ul style="text-align: left;">
                        <li><strong>Field Paths:</strong> Complete dot-notation paths to every field</li>
                        <li><strong>Data Types:</strong> Type classification (string, integer, object, array, etc.)</li>
                        <li><strong>Frequency:</strong> How often each field appears in your data</li>
                        <li><strong>Examples:</strong> Sanitized example values</li>
                        <li><strong>Relationships:</strong> How Orders and Catalog APIs connect</li>
                        <li><strong>Integration Guide:</strong> Most common fields for API integration</li>
                    </ul>
                </div>
                
                <div style="text-align: center; margin-top: 30px;">
                    <a href="/explorer/discover/{merchant_id}" style="background: #ffc107; color: black; 
                       padding: 12px 24px; text-decoration: none; border-radius: 5px; margin: 5px;">
                        🔄 Run Again
                    </a>
                    <a href="/explorer" style="background: #6c757d; color: white; padding: 12px 24px; 
                       text-decoration: none; border-radius: 5px; margin: 5px;">
                        ← Back to Explorer
                    </a>
                </div>
            </div>
            '''
            
        except Exception as e:
            print(f"❌ Discovery error: {e}")
            return f'''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
                <h2 style="color: #dc3545;">❌ Discovery Failed</h2>
                <div style="background: #f8d7da; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <p><strong>Error:</strong> {str(e)}</p>
                </div>
                <a href="/explorer/discover/{merchant_id}" style="background: #28a745; color: white; 
                   padding: 12px 24px; text-decoration: none; border-radius: 5px;">Try Again</a>
            </div>
            ''', 500
    
    @app.route('/explorer/download-csv/<merchant_id>', methods=['POST'])
    def download_field_csv(merchant_id):
        """Download the generated CSV report"""
        try:
            csv_data = request.form.get('csv_data')
            filename = request.form.get('filename', f'square_fields_{merchant_id}.csv')
            
            if not csv_data:
                return 'No CSV data provided', 400
            
            return Response(
                csv_data,
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
            
        except Exception as e:
            return f'Download failed: {str(e)}', 500
    
    @app.route('/explorer/quick-export/<merchant_id>')
    def quick_export_fields(merchant_id):
        """Quick field discovery and immediate CSV export"""
        try:
            # Verify merchant
            tokens = sync_instance.get_tokens(merchant_id)
            if not tokens:
                return f'Merchant {merchant_id} not found', 404
            
            merchant_name = tokens.get('merchant_name', 'Unknown')
            
            # Show loading page with auto-redirect to start discovery
            return f'''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
                <h2>⚡ Quick Field Export</h2>
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <p><strong>Merchant:</strong> {merchant_name}</p>
                    <p><strong>Process:</strong> Running field discovery with default settings...</p>
                </div>
                
                <div style="background: #fff3cd; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <p>🔄 This will take 2-3 minutes to complete</p>
                    <p>Your CSV download will start automatically when ready</p>
                </div>
                
                <div id="progress" style="margin: 20px 0;">
                    <div style="background: #e9ecef; height: 20px; border-radius: 10px; overflow: hidden;">
                        <div id="progress-bar" style="background: #007bff; height: 100%; width: 0%; 
                             transition: width 2s ease-in-out;"></div>
                    </div>
                    <p id="status">Initializing...</p>
                </div>
                
                <script>
                    let progress = 0;
                    const progressBar = document.getElementById('progress-bar');
                    const status = document.getElementById('status');
                    
                    const steps = [
                        'Fetching order samples...',
                        'Analyzing order fields...',
                        'Fetching catalog samples...',
                        'Analyzing catalog fields...',
                        'Mapping relationships...',
                        'Generating CSV...',
                        'Complete!'
                    ];
                    
                    function updateProgress() {{
                        if (progress < steps.length - 1) {{
                            status.textContent = steps[progress];
                            progressBar.style.width = (progress / (steps.length - 1)) * 100 + '%';
                            progress++;
                            setTimeout(updateProgress, 20000); // 20 seconds per step
                        }} else {{
                            status.textContent = 'Complete! Starting download...';
                            progressBar.style.width = '100%';
                            // Trigger actual discovery
                            setTimeout(() => {{
                                window.location.href = '/explorer/execute-quick-export/{merchant_id}';
                            }}, 2000);
                        }}
                    }}
                    
                    // Start progress simulation
                    setTimeout(updateProgress, 1000);
                </script>
                
                <div style="margin-top: 20px;">
                    <a href="/explorer" style="color: #6c757d; text-decoration: none;">← Cancel and go back</a>
                </div>
            </div>
            '''
            
        except Exception as e:
            return f'Error: {str(e)}', 500
    
    @app.route('/explorer/execute-quick-export/<merchant_id>')
    def execute_quick_export(merchant_id):
        """Actually execute the field discovery and return CSV"""
        try:
            # Create explorer and run discovery
            explorer = SquareFieldExplorer(sync_instance)
            explorer.explore_merchant_data(merchant_id, sample_size=35)  # Smaller sample for quick export
            
            # Generate CSV
            csv_data = explorer.export_field_mapping_csv()
            filename = f'square_api_fields_{merchant_id}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
            
            return Response(
                csv_data,
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
            
        except Exception as e:
            print(f"❌ Quick export error: {e}")
            return f'''
            <div style="max-width: 600px; margin: 50px auto; padding: 30px; text-align: center;">
                <h2 style="color: #dc3545;">❌ Export Failed</h2>
                <div style="background: #f8d7da; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <p><strong>Error:</strong> {str(e)}</p>
                    <p>This might be due to:</p>
                    <ul style="text-align: left; display: inline-block;">
                        <li>Rate limiting from Square API</li>
                        <li>Insufficient data in merchant account</li>
                        <li>Token expiration</li>
                    </ul>
                </div>
                <a href="/explorer" style="background: #007bff; color: white; padding: 12px 24px; 
                   text-decoration: none; border-radius: 5px;">Back to Explorer</a>
            </div>
            ''', 500
    
    @app.route('/explorer/api/discover/<merchant_id>')
    def api_discover_fields(merchant_id):
        """API endpoint for programmatic field discovery"""
        try:
            sample_size = int(request.args.get('sample_size', 50))
            
            explorer = SquareFieldExplorer(sync_instance)
            discovered_fields = explorer.explore_merchant_data(merchant_id, sample_size)
            summary = explorer.get_field_summary()
            
            return jsonify({
                'status': 'success',
                'merchant_id': merchant_id,
                'discovery_timestamp': discovered_fields['discovery_timestamp'],
                'summary': summary,
                'field_counts': {
                    'orders_fields': len(discovered_fields['orders_fields']),
                    'catalog_fields': len(discovered_fields['catalog_fields']),
                    'total_fields': len(discovered_fields['orders_fields']) + len(discovered_fields['catalog_fields'])
                },
                'data_types_found': list(summary['data_type_distribution'].keys()),
                'sample_size_used': sample_size
            })
            
        except Exception as e:
            return jsonify({
                'status': 'error',
                'error': str(e),
                'merchant_id': merchant_id
            }), 500
    
    @app.route('/explorer/status')
    def explorer_status():
        """Explorer service status"""
        merchants = sync_instance.get_all_merchants()
        
        return jsonify({
            'status': 'active',
            'connected_merchants': len(merchants),
            'available_merchants': [
                {
                    'merchant_id': m['merchant_id'],
                    'merchant_name': m.get('merchant_name', 'Unknown'),
                    'total_customers': m.get('total_customers', 0),
                    'last_sync': m.get('last_sync', 'Never')
                }
                for m in merchants
            ],
            'timestamp': datetime.now().isoformat()
        })

def add_explorer_background_task():
    """Optional: Add background field discovery scheduling"""
    def background_field_discovery():
        """Run field discovery for all merchants periodically"""
        print("🔍 Background field discovery service started")
        
        while True:
            try:
                # Run field discovery for all merchants once per week
                time.sleep(7 * 24 * 3600)  # 7 days
                
                merchants = sync_instance.get_all_merchants()
                for merchant in merchants:
                    merchant_id = merchant['merchant_id']
                    print(f"🔍 Running weekly field discovery for {merchant_id}")
                    
                    try:
                        explorer = SquareFieldExplorer(sync_instance)
                        explorer.explore_merchant_data(merchant_id, sample_size=30)
                        print(f"✅ Field discovery complete for {merchant_id}")
                    except Exception as e:
                        print(f"❌ Field discovery failed for {merchant_id}: {e}")
                    
                    time.sleep(300)  # 5 minutes between merchants
                    
            except Exception as e:
                print(f"❌ Background field discovery error: {e}")
                time.sleep(3600)  # Sleep 1 hour on error
    
    return threading.Thread(target=background_field_discovery, daemon=True)