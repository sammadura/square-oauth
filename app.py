from flask import Flask, redirect, request
import requests
import os

app = Flask(__name__)

@app.route('/')
def home():
    return '<a href="/signin">Connect with Square</a>'

@app.route('/signin')
def signin():
    client_id = os.environ.get('SQUARE_CLIENT_ID')
    # Use Square Sandbox for testing
    base_url = 'https://connect.squareupsandbox.com'
    redirect_uri = os.environ.get('SQUARE_REDIRECT_URI')  # e.g., https://your-app.onrender.com/oauth2callback
    scope = 'MERCHANT_PROFILE_READ PAYMENTS_WRITE'
    return redirect(f'{base_url}/oauth2/authorize?client_id={client_id}&redirect_uri={redirect_uri}&scope={scope}')

@app.route('/oauth2callback')
def oauth2callback():
    code = request.args.get('code')
    if not code:
        return 'Error: No authorization code provided', 400
    
    client_id = os.environ.get('SQUARE_CLIENT_ID')
    client_secret = os.environ.get('SQUARE_CLIENT_SECRET')
    redirect_uri = os.environ.get('SQUARE_REDIRECT_URI')
    
    response = requests.post('https://connect.squareupsandbox.com/oauth2/token', data={
        'client_id': client_id,
        'client_secret': client_secret,
        'code': code,
        'grant_type': 'authorization_code',
        'redirect_uri': redirect_uri
    })
    
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data.get('access_token')
        # In a real app, store the access_token securely (e.g., in a database)
        return f'Authorization successful! Access Token: {access_token}'
    else:
        return f'Authorization failed: {response.text}', response.status_code

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))