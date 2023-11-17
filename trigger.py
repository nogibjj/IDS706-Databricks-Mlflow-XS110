# When commit&push, trigger the jobs automatically
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Replace with your environment variables or provide the actual values
server_hostname = os.getenv("DATABRICKS_HOST")
job_id = os.getenv("JOB_ID")
access_token = os.getenv("DATABRICKS_TOKEN")
url = f'https://{server_hostname}/api/2.0/jobs/run-now'

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
}

data = {
    'job_id': job_id
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    print('Job run successfully triggered')
else:
    print(f'Error: {response.status_code}, {response.text}')