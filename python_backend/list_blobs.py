#!/usr/bin/env python3
import os
from azure.storage.blob import BlobServiceClient

# Set up Azure Storage credentials
account_url = "https://infolder.blob.core.windows.net"
account_key = "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw=="
connection_string = f"DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey={account_key};EndpointSuffix=core.windows.net;BlobEndpoint={account_url}/"

# Create client
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# List containers
print("\n=== Containers ===")
containers = blob_service_client.list_containers()
for container in containers:
    print(f"- {container.name}")
    
    # List blobs in this container
    container_client = blob_service_client.get_container_client(container.name)
    print("\n  Blobs:")
    blobs = container_client.list_blobs()
    count = 0
    for blob in blobs:
        print(f"  - {blob.name}")
        count += 1
        if count >= 10:
            print("  ...more blobs exist (showing first 10 only)")
            break
    if count == 0:
        print("  (no blobs found)")
    print()
