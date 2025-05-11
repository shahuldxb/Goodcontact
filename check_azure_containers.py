#!/usr/bin/env python3
"""
Check Azure Storage Containers and Files
"""

from python_backend.azure_storage_service import AzureStorageService

def main():
    service = AzureStorageService()
    
    # List all containers
    print("Checking available containers:")
    containers = list(service.blob_service_client.list_containers())
    for container in containers:
        print(f"- {container.name}")
    
    print(f"\nTotal containers found: {len(containers)}")
    
    # If no containers found, the connection might be wrong or there are no containers
    if not containers:
        print("\nNo containers found. This could be due to:")
        print("1. The storage account connection string is incorrect")
        print("2. The storage account doesn't have any containers")
        print("3. The authentication credentials don't have access to list containers")
    
    # Check each known container for files
    known_containers = ["shahulin", "shahulout", "demoin", "demoout"]
    for container_name in known_containers:
        try:
            print(f"\nChecking files in container '{container_name}':")
            container_client = service.blob_service_client.get_container_client(container_name)
            exists = container_client.exists()
            
            if not exists:
                print(f"Container '{container_name}' does not exist")
                continue
                
            blobs = list(container_client.list_blobs())
            print(f"Found {len(blobs)} files in '{container_name}'")
            
            # List first 5 files
            for i, blob in enumerate(blobs[:5]):
                print(f"  {i+1}. {blob.name} ({blob.size} bytes)")
                
            if not blobs:
                print(f"  Container '{container_name}' appears to be empty")
        except Exception as e:
            print(f"Error accessing container '{container_name}': {e}")

if __name__ == "__main__":
    main()