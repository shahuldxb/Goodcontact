#!/usr/bin/env python3
"""
Test generating SAS URL for a file in shahulin container
with very long expiry time
"""
from python_backend.azure_storage_service import AzureStorageService
from datetime import datetime, timedelta
import requests
import sys

def generate_long_sas_url(blob_name, expiry_hours=240):
    """Generate SAS URL with long expiry time"""
    service = AzureStorageService()
    
    # Get container client
    container_client = service.blob_service_client.get_container_client('shahulin')
    blob_client = container_client.get_blob_client(blob_name)
    
    # Calculate expiry time
    start_time = datetime.utcnow() - timedelta(minutes=5)  # Start 5 minutes ago to avoid clock skew
    expiry_time = datetime.utcnow() + timedelta(hours=expiry_hours)
    
    # Generate SAS token with read permission
    from azure.storage.blob import generate_blob_sas, BlobSasPermissions
    
    sas_token = generate_blob_sas(
        account_name=service.blob_service_client.account_name,
        container_name='shahulin',
        blob_name=blob_name,
        account_key=service.blob_service_client.credential.account_key,
        permission=BlobSasPermissions(read=True),
        start=start_time,
        expiry=expiry_time
    )
    
    # Construct full URL
    sas_url = f"{blob_client.url}?{sas_token}"
    return sas_url

def test_sas_url(sas_url):
    """Test if the SAS URL works by making a HEAD request"""
    try:
        response = requests.head(sas_url, timeout=10)
        print(f"SAS URL is valid: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type')}")
        print(f"Content-Length: {response.headers.get('Content-Length')} bytes")
        return response.status_code == 200
    except Exception as e:
        print(f"Error testing SAS URL: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_sas_gen.py <blob_name>")
        sys.exit(1)
        
    blob_name = sys.argv[1]
    print(f"Generating SAS URL for blob: {blob_name}")
    
    sas_url = generate_long_sas_url(blob_name)
    print(f"SAS URL: {sas_url}")
    
    print("\nTesting SAS URL...")
    test_sas_url(sas_url)