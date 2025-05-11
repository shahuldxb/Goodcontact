#!/usr/bin/env python3
"""
Check file in shahulin container
"""
import os
import sys
from python_backend.azure_storage_service import AzureStorageService

def check_blob(blob_name):
    service = AzureStorageService()
    print(f"Checking blob: {blob_name}")
    
    try:
        content = service.download_blob('shahulin', blob_name)
        print(f"Downloaded {len(content)} bytes")
        
        sample_dir = '/tmp/sample_check'
        os.makedirs(sample_dir, exist_ok=True)
        sample_path = os.path.join(sample_dir, blob_name)
        
        with open(sample_path, 'wb') as f:
            f.write(content)
        
        print(f"Saved sample to {sample_path}")
        os.system(f"file {sample_path}")
        
        # Check file size
        stat_info = os.stat(sample_path)
        print(f"File size: {stat_info.st_size} bytes")
        
        # Print first 20 bytes as hex to check if it's a valid file header
        with open(sample_path, 'rb') as f:
            header = f.read(20)
        
        print("File header (hex):")
        print(' '.join(f'{b:02x}' for b in header))
        
        return True
    except Exception as e:
        print(f"Error checking blob: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_file.py <blob_name>")
        sys.exit(1)
    
    blob_name = sys.argv[1]
    check_blob(blob_name)