import requests
import os

# make sure you configure these three variables correctly before you try to run the code 
AZURE_ENDPOINT_URL='https://login.microsoftonline.com/d91c9061-f3aa-4e68-a16d-6ce3c872378e/oauth2/token'                                                  
AZURE_APP_ID='fb92184f-c631-47e4-8829-65db1201c6b4'
AZURE_APP_SECRET='myapp'

def get_token_from_client_credentials(endpoint, client_id, client_secret):
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://management.core.windows.net/',
    }
    response = requests.post(endpoint, data=payload).json()
    return response['access_token']

# test
if __name__ == '__main__':
    auth_token = get_token_from_client_credentials(endpoint=AZURE_ENDPOINT_URL,
            client_id=AZURE_APP_ID,
            client_secret=AZURE_APP_SECRET)
    print auth_token