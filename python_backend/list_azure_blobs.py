#!/usr/bin/env python3
"""
Utility script to list available blobs in Azure Storage containers.
This script helps verify the existence of specific files for transcription testing.
"""

import os
import sys
import logging
from azure_storage_service import AzureStorageService

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def list_container_blobs(container_name, limit=20, filter_suffix=None):
    """
    List blobs in a specific container.
    
    Args:
        container_name (str): Container name
        limit (int): Maximum number of blobs to list
        filter_suffix (str): Optional file extension filter (e.g., '.mp3')
        
    Returns:
        list: List of blob information dictionaries
    """
    try:
        # Initialize storage service
        service = AzureStorageService()
        
        # Get blobs
        blobs = service.list_blobs(container_name)
        
        # Filter by suffix if specified
        if filter_suffix:
            blobs = [blob for blob in blobs if blob['name'].lower().endswith(filter_suffix.lower())]
        
        # Print blob information
        print(f"\nContents of '{container_name}' container:")
        print("-" * 80)
        
        # Check if any blobs were found
        if not blobs:
            print(f"No blobs found in container '{container_name}'")
            return []
            
        # Limit the number of blobs to display
        displayed_blobs = blobs[:limit]
        
        # Display blob information
        for i, blob in enumerate(displayed_blobs):
            size_kb = blob['size'] / 1024
            size_mb = size_kb / 1024
            
            if size_mb >= 1:
                size_str = f"{size_mb:.2f} MB"
            else:
                size_str = f"{size_kb:.2f} KB"
                
            print(f"{i+1}. {blob['name']} ({size_str})")
            
        # Show count of remaining blobs
        remaining = len(blobs) - limit
        if remaining > 0:
            print(f"... and {remaining} more blob(s)")
            
        print("-" * 80)
        return blobs
        
    except Exception as e:
        logger.error(f"Error listing blobs in container '{container_name}': {str(e)}")
        return []

def generate_test_sas_urls(blob_names, container_name="shahulin", expiry_hours=24):
    """
    Generate SAS URLs for specified blob names.
    
    Args:
        blob_names (list): List of blob names to generate SAS URLs for
        container_name (str): Container name
        expiry_hours (int): SAS URL expiry hours
        
    Returns:
        dict: Dictionary mapping blob names to SAS URLs
    """
    try:
        # Initialize storage service
        service = AzureStorageService()
        
        # Generate SAS URLs
        results = {}
        for blob_name in blob_names:
            sas_url = service.generate_sas_url(container_name, blob_name, expiry_hours)
            if sas_url:
                results[blob_name] = sas_url
                print(f"Generated SAS URL for '{blob_name}' (expires in {expiry_hours} hours)")
            else:
                print(f"Failed to generate SAS URL for '{blob_name}'")
                
        return results
        
    except Exception as e:
        logger.error(f"Error generating SAS URLs: {str(e)}")
        return {}

def main():
    """
    Main function to list blobs and generate test SAS URLs.
    """
    print("\nAzure Blob Storage Utility")
    print("=========================")
    
    # List blobs in source container (shahulin)
    source_blobs = list_container_blobs("shahulin", limit=10)
    
    # List blobs in destination container (shahulout)
    destination_blobs = list_container_blobs("shahulout", limit=10)
    
    # Check if any audio files exist
    audio_extensions = ['.mp3', '.wav', '.m4a', '.ogg', '.flac']
    
    print("\nAudio files in source container:")
    audio_found = False
    
    for ext in audio_extensions:
        filtered_blobs = [blob for blob in source_blobs if blob['name'].lower().endswith(ext)]
        if filtered_blobs:
            audio_found = True
            print(f"\nFiles with extension {ext}:")
            for i, blob in enumerate(filtered_blobs[:5]):  # Show only first 5
                print(f"  {i+1}. {blob['name']}")
                
    if not audio_found:
        print("No audio files found in the source container.")
    
    # Print usage information
    print("\nUsage for testing:")
    print("-" * 80)
    print("To test a specific file, use:")
    print("python test_direct_transcribe.py <blob_name>")
    print("Example: python test_direct_transcribe.py sample.mp3")
    
    # If audio files were found, provide a concrete example
    if audio_found and len(filtered_blobs) > 0:
        sample_file = filtered_blobs[0]['name']
        print(f"\nExample using an available file:")
        print(f"python test_direct_transcribe.py {sample_file}")
    
if __name__ == "__main__":
    main()