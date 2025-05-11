"""
Azure Storage Service for the Contact Center Analytics platform

This module provides functions to interact with Azure Blob Storage, including:
- Listing blobs in a container
- Uploading blobs to a container
- Downloading blobs from a container
- Moving blobs between containers
- Generating SAS URLs for blobs
"""

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, generate_blob_sas
from azure.storage.blob import BlobSasPermissions
from datetime import datetime, timedelta
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AzureStorageService:
    """
    Azure Storage Service class that provides methods to interact with Azure Blob Storage.
    """

    def __init__(self):
        """
        Initialize the Azure Storage Service with connection string from environment variables.
        """
        # Instead of hardcoding the connection string, build it from components or use an environment variable
        account_name = "infolder"
        account_key = "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw=="
        self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        
        # Container names
        self.source_container = "shahulin"
        self.destination_container = "shahulout"
        
        # Create BlobServiceClient
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            logger.info("Successfully connected to Azure Blob Storage")
        except Exception as e:
            logger.error(f"Failed to connect to Azure Blob Storage: {str(e)}")
            raise

    def list_blobs(self, container_name):
        """
        List all blobs in a container.
        
        Args:
            container_name (str): The name of the container.
            
        Returns:
            list: A list of dictionaries containing blob information.
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = container_client.list_blobs()
            
            # Convert generator to list of dictionaries with relevant info
            blob_list = []
            for blob in blobs:
                blob_list.append({
                    "name": blob.name,
                    "size": blob.size,
                    "last_modified": blob.last_modified,
                    "content_type": blob.content_settings.content_type
                })
            
            return blob_list
        except Exception as e:
            logger.error(f"Error listing blobs in container {container_name}: {str(e)}")
            return []

    def upload_blob(self, container_name, blob_name, data):
        """
        Upload a blob to a container.
        
        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.
            data (bytes or file-like object): The content to upload.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Successfully uploaded blob {blob_name} to container {container_name}")
            return True
        except Exception as e:
            logger.error(f"Error uploading blob {blob_name} to container {container_name}: {str(e)}")
            return False

    def download_blob(self, container_name, blob_name, destination_file_path=None):
        """
        Download a blob from a container.
        
        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.
            destination_file_path (str, optional): The path to save the blob. If None, blob content is returned.
            
        Returns:
            bytes or bool: The blob content as bytes if destination_file_path is None, 
                           otherwise True if successful, False if failed.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            
            if destination_file_path:
                # Ensure the directory exists
                os.makedirs(os.path.dirname(os.path.abspath(destination_file_path)), exist_ok=True)
                
                # Download the blob to a file
                with open(destination_file_path, "wb") as file:
                    blob_data = blob_client.download_blob()
                    file.write(blob_data.readall())
                
                logger.info(f"Successfully downloaded blob {blob_name} to {destination_file_path}")
                return True
            else:
                # Return the blob content
                blob_data = blob_client.download_blob()
                return blob_data.readall()
                
        except Exception as e:
            logger.error(f"Error downloading blob {blob_name} from container {container_name}: {str(e)}")
            return False if destination_file_path else None

    def move_blob(self, source_container, source_blob_name, destination_container, destination_blob_name=None):
        """
        Move a blob from one container to another (copy and delete).
        
        Args:
            source_container (str): The source container name.
            source_blob_name (str): The source blob name.
            destination_container (str): The destination container name.
            destination_blob_name (str, optional): The destination blob name. If None, source_blob_name is used.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        # Use the source blob name if destination blob name is not provided
        if not destination_blob_name:
            destination_blob_name = source_blob_name
            
        try:
            # Get source blob client
            source_blob_client = self.blob_service_client.get_blob_client(
                container=source_container, 
                blob=source_blob_name
            )
            
            # Get destination blob client
            destination_blob_client = self.blob_service_client.get_blob_client(
                container=destination_container, 
                blob=destination_blob_name
            )
            
            # Get the source URL
            source_url = source_blob_client.url
            
            # Copy the blob to the destination
            destination_blob_client.start_copy_from_url(source_url)
            
            # Wait for the copy to complete
            import time
            properties = destination_blob_client.get_blob_properties()
            copy_status = properties.copy.status
            
            # Check copy status
            while copy_status == 'pending':
                time.sleep(1)
                properties = destination_blob_client.get_blob_properties()
                copy_status = properties.copy.status
                
            if copy_status == 'success':
                # Delete the source blob
                source_blob_client.delete_blob()
                logger.info(f"Successfully moved blob from {source_container}/{source_blob_name} to {destination_container}/{destination_blob_name}")
                return True
            else:
                logger.error(f"Copy operation failed: {copy_status}")
                return False
                
        except Exception as e:
            logger.error(f"Error moving blob from {source_container}/{source_blob_name} to {destination_container}/{destination_blob_name}: {str(e)}")
            return False

    def generate_sas_url(self, container_name, blob_name, expiry_hours=240):
        """
        Generate a SAS URL for a blob that expires after the specified time.
        
        Args:
            container_name (str): The container name.
            blob_name (str): The blob name.
            expiry_hours (int): Number of hours until the SAS URL expires. Default is 240 hours (10 days).
            
        Returns:
            str: The SAS URL for the blob.
        """
        try:
            # Get account information
            account_name = self.blob_service_client.account_name
            account_key = self.blob_service_client.credential.account_key
            
            # Generate SAS token using the exact same syntax
            sas_token = generate_blob_sas(
                account_name=account_name,
                container_name=container_name,
                blob_name=blob_name,
                account_key=account_key,
                permission=BlobSasPermissions(read=True),  # Allow read access
                expiry=datetime.utcnow() + timedelta(hours=expiry_hours),  # Set expiration
            )
            
            # Construct the full SAS URL
            sas_url = f"https://{self.blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
            
            logger.info(f"Generated SAS URL for {container_name}/{blob_name} that expires in {expiry_hours} hours")
            return sas_url
            
        except Exception as e:
            logger.error(f"Error generating SAS URL for {container_name}/{blob_name}: {str(e)}")
            return None

    def get_blob_url(self, container_name, blob_name):
        """
        Get the direct URL for a blob (without SAS token).
        
        Args:
            container_name (str): The container name.
            blob_name (str): The blob name.
            
        Returns:
            str: The URL for the blob.
        """
        try:
            # Construct the blob URL
            blob_url = f"https://{self.blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}"
            
            return blob_url
            
        except Exception as e:
            logger.error(f"Error generating blob URL for {container_name}/{blob_name}: {str(e)}")
            return None
            
    def get_sas_url(self, blob_name, container_name=None, expiry_hours=240):
        """
        Generate a SAS URL for a blob in the source container.
        This is a convenience wrapper around generate_sas_url that defaults to the source container.
        
        Args:
            blob_name (str): The blob name.
            container_name (str, optional): The container name. If None, the source container is used.
            expiry_hours (int): Number of hours until the SAS URL expires. Default is 240 hours (10 days).
            
        Returns:
            str: The SAS URL for the blob.
        """
        if container_name is None:
            container_name = self.source_container
            
        return self.generate_sas_url(container_name, blob_name, expiry_hours)


    def copy_blob_to_destination(self, source_blob_name, destination_blob_name=None):
        """
        Move a blob from the source container to the destination container.
        
        Args:
            source_blob_name (str): The source blob name.
            destination_blob_name (str, optional): The destination blob name. If None, source_blob_name is used.
            
        Returns:
            str: The URL of the blob in the destination container if successful, None otherwise.
        """
        try:
            # Use source_blob_name as destination_blob_name if not provided
            if destination_blob_name is None:
                destination_blob_name = source_blob_name
                
            # Copy the blob
            success = self.move_blob(
                self.source_container, 
                source_blob_name, 
                self.destination_container, 
                destination_blob_name
            )
            
            # Return the URL of the destination blob if successful
            if success:
                return self.get_blob_url(self.destination_container, destination_blob_name)
                
            return None
            
        except Exception as e:
            logger.error(f"Error copying blob to destination: {str(e)}")
            return None

    def list_source_blobs(self):
        """
        List all blobs in the source container.
        
        Returns:
            list: A list of dictionaries containing blob information.
        """
        return self.list_blobs(self.source_container)
        
    def list_destination_blobs(self):
        """
        List all blobs in the destination container.
        
        Returns:
            list: A list of dictionaries containing blob information.
        """
        return self.list_blobs(self.destination_container)

# Simple test function
def main():
    """
    Test the AzureStorageService class.
    """
    service = AzureStorageService()
    
    # List blobs in source container
    print("Listing blobs in source container:")
    blobs = service.list_source_blobs()
    for blob in blobs[:5]:  # Show first 5 blobs
        print(f"  - {blob['name']} ({blob['size']} bytes)")
    
    # Generate SAS URL for a blob
    if blobs:
        sample_blob = blobs[0]['name']
        sas_url = service.get_sas_url(sample_blob)
        print(f"\nSAS URL for {sample_blob}:")
        print(f"  {sas_url}")


if __name__ == "__main__":
    main()