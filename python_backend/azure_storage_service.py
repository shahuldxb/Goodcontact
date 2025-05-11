from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, generate_blob_sas, BlobSasPermissions
from azure.identity import DefaultAzureCredential
from datetime import datetime, timedelta
import os
import logging
import json

class AzureStorageService:
    def __init__(self):
        """Initialize the Azure Storage Service"""
        self.logger = logging.getLogger(__name__)
        
        # Azure Storage account configuration
        self.storage_account_url = os.environ.get("AZURE_STORAGE_ACCOUNT_URL", "https://infolder.blob.core.windows.net")
        self.storage_account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==")
        self.source_container_name = os.environ.get("AZURE_SOURCE_CONTAINER_NAME", "shahulin")
        self.destination_container_name = os.environ.get("AZURE_DESTINATION_CONTAINER_NAME", "shahulout")
        
        # Initialize BlobServiceClient
        try:
            self.blob_service_client = BlobServiceClient(
                account_url=self.storage_account_url,
                credential=self.storage_account_key
            )
            self.source_container_client = self.blob_service_client.get_container_client(self.source_container_name)
            self.destination_container_client = self.blob_service_client.get_container_client(self.destination_container_name)
            
            # Ensure containers exist
            self._ensure_containers_exist()
            
            self.logger.info("Azure Storage Service initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing Azure Storage Service: {str(e)}")
            raise
    
    def _ensure_containers_exist(self):
        """Ensure that the source and destination containers exist"""
        try:
            # Create source container if it doesn't exist
            if not self.source_container_client.exists():
                self.source_container_client.create_container()
                self.logger.info(f"Created source container: {self.source_container_name}")
            
            # Create destination container if it doesn't exist
            if not self.destination_container_client.exists():
                self.destination_container_client.create_container()
                self.logger.info(f"Created destination container: {self.destination_container_name}")
        except Exception as e:
            self.logger.error(f"Error ensuring containers exist: {str(e)}")
            raise
    
    def list_source_blobs(self):
        """List blobs in the source container"""
        try:
            blobs = []
            for blob in self.source_container_client.list_blobs():
                # Generate SAS URL
                blob_client = self.blob_service_client.get_blob_client(
                    container=self.source_container_name,
                    blob=blob.name
                )
                
                # Get blob properties
                props = blob_client.get_blob_properties()
                
                blobs.append({
                    "name": blob.name,
                    "url": blob_client.url,
                    "size": props.size,
                    "lastModified": props.last_modified.isoformat()
                })
            
            self.logger.info(f"Listed {len(blobs)} blobs in source container")
            return blobs
        except Exception as e:
            self.logger.error(f"Error listing source blobs: {str(e)}")
            raise
    
    def list_destination_blobs(self):
        """List blobs in the destination container"""
        try:
            blobs = []
            for blob in self.destination_container_client.list_blobs():
                # Generate SAS URL
                blob_client = self.blob_service_client.get_blob_client(
                    container=self.destination_container_name,
                    blob=blob.name
                )
                
                # Get blob properties
                props = blob_client.get_blob_properties()
                
                blobs.append({
                    "name": blob.name,
                    "url": blob_client.url,
                    "size": props.size,
                    "lastModified": props.last_modified.isoformat()
                })
            
            self.logger.info(f"Listed {len(blobs)} blobs in destination container")
            return blobs
        except Exception as e:
            self.logger.error(f"Error listing destination blobs: {str(e)}")
            raise
    
    def download_blob(self, blob_name, local_path):
        """Download a blob from the source container to a local file"""
        try:
            # Get blob client
            blob_client = self.blob_service_client.get_blob_client(
                container=self.source_container_name,
                blob=blob_name
            )
            
            # Download blob
            with open(local_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            
            self.logger.info(f"Downloaded blob {blob_name} to {local_path}")
            return local_path
        except Exception as e:
            self.logger.error(f"Error downloading blob {blob_name}: {str(e)}")
            raise
    
    def copy_blob_to_destination(self, blob_name):
        """Copy a blob from the source container to the destination container"""
        try:
            # Get source blob client
            source_blob_client = self.blob_service_client.get_blob_client(
                container=self.source_container_name,
                blob=blob_name
            )
            
            # Get destination blob client
            dest_blob_client = self.blob_service_client.get_blob_client(
                container=self.destination_container_name,
                blob=blob_name
            )
            
            # Copy blob
            dest_blob_client.start_copy_from_url(source_blob_client.url)
            
            self.logger.info(f"Copied blob {blob_name} to destination container")
            return True
        except Exception as e:
            self.logger.error(f"Error copying blob {blob_name} to destination: {str(e)}")
            raise
    
    def generate_sas_url(self, container_name, blob_name, hours=24):
        """Generate a SAS URL for a blob"""
        try:
            # Get blob client
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            
            # Generate SAS token
            sas_token = generate_blob_sas(
                account_name=self.blob_service_client.account_name,
                container_name=container_name,
                blob_name=blob_name,
                account_key=self.storage_account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=hours)
            )
            
            # Create SAS URL
            sas_url = f"{blob_client.url}?{sas_token}"
            
            self.logger.info(f"Generated SAS URL for {blob_name} in {container_name}")
            return sas_url
        except Exception as e:
            self.logger.error(f"Error generating SAS URL for {blob_name}: {str(e)}")
            raise