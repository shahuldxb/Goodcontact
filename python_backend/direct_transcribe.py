#!/usr/bin/env python3
"""
DirectTranscribe Class
A pure REST API approach for direct transcription of Azure Blob files using SAS URLs.
This class implements the same successful pattern from test_direct_transcription.py
but as a reusable class for production use.
"""

import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DirectTranscribe:
    """
    DirectTranscribe class for transcribing Azure Blob Storage files
    using Deepgram's REST API with SAS URLs.
    """
    
    def __init__(self, deepgram_api_key=None, connection_string=None):
        """
        Initialize the DirectTranscribe class.
        
        Args:
            deepgram_api_key: The Deepgram API key. If None, will use environment variable.
            connection_string: Azure Storage connection string. If None, will use environment variable.
        """
        # Setup Deepgram API key
        self.api_key = deepgram_api_key
        if not self.api_key:
            self.api_key = os.environ.get('DEEPGRAM_API_KEY')
            if not self.api_key:
                # Fallback to known working key as last resort
                self.api_key = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"
                logger.warning("Using fallback Deepgram API key, please set DEEPGRAM_API_KEY")
        
        # Setup Azure Storage connection
        self.connection_string = connection_string
        if not self.connection_string:
            self.connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            if not self.connection_string:
                # Fallback to known working connection string as last resort
                self.connection_string = "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
                logger.warning("Using fallback Azure Storage connection string, please set AZURE_STORAGE_CONNECTION_STRING")
                
        # Set up Azure Storage module - import here to allow for lazy loading
        try:
            from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
            self.BlobServiceClient = BlobServiceClient
            self.generate_blob_sas = generate_blob_sas
            self.BlobSasPermissions = BlobSasPermissions
            logger.info("Azure Storage module imported successfully")
        except ImportError:
            logger.error("Azure Storage module not found. Please install with: pip install azure-storage-blob")
            raise ImportError("Azure Storage module not found")
        
        # Initialize the Azure Storage client
        try:
            self.blob_service_client = self.BlobServiceClient.from_connection_string(self.connection_string)
            logger.info("Connected to Azure Blob Storage successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Azure Blob Storage: {str(e)}")
            raise
    
    def generate_sas_url(self, container_name, blob_name, expiry_hours=240):
        """
        Generate a SAS URL for a blob with a long expiry time.
        This implementation matches the successful pattern from test_sas_gen.py.
        
        Args:
            container_name: Name of the Azure Storage container
            blob_name: Name of the blob (file)
            expiry_hours: Hours until the SAS URL expires (default: 240 hours = 10 days)
            
        Returns:
            str: SAS URL for the blob
        """
        logger.info(f"Generating SAS URL for blob: {blob_name} in container: {container_name} with {expiry_hours} hour expiry")
        
        try:
            # Get the container client
            container_client = self.blob_service_client.get_container_client(container_name)
            
            # Make sure the blob exists
            blob_client = container_client.get_blob_client(blob_name)
            
            try:
                # This will raise an error if the blob doesn't exist
                blob_properties = blob_client.get_blob_properties()
                logger.info(f"Blob properties: {blob_properties.size} bytes, {blob_properties.content_type}")
            except Exception as e:
                logger.error(f"Blob {blob_name} does not exist in container {container_name}: {str(e)}")
                raise ValueError(f"Blob {blob_name} does not exist in container {container_name}")
            
            # Calculate expiry time
            start_time = datetime.utcnow()
            expiry_time = start_time + timedelta(hours=expiry_hours)
            
            # Get account details from connection string
            account_name = self.connection_string.split(';')[1].split('=')[1]
            account_key = self.connection_string.split(';')[2].split('=')[1]
            
            # Define permissions
            permissions = self.BlobSasPermissions(read=True)
            
            # Generate SAS token
            sas_token = self.generate_blob_sas(
                account_name=account_name,
                container_name=container_name,
                blob_name=blob_name,
                account_key=account_key,
                permission=permissions,
                expiry=expiry_time,
                start=start_time
            )
            
            # Construct the full SAS URL
            sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
            
            # Log a truncated version for security
            logger.info(f"Generated SAS URL with expiry time: {expiry_time.isoformat()}")
            logger.info(f"SAS URL prefix: {sas_url[:60]}...")
            
            return sas_url
            
        except Exception as e:
            logger.error(f"Error generating SAS URL: {str(e)}")
            raise
    
    def transcribe(self, blob_name, container_name="shahulin", model="nova-2", 
                   diarize=True, summarize=True, language_detection=True):
        """
        Transcribe an audio file from Azure Blob Storage using Deepgram's REST API.
        This method generates a SAS URL and sends it to Deepgram for processing.
        No audio file is downloaded - the transcription happens directly from the SAS URL.
        
        Args:
            blob_name: Name of the blob in Azure Storage
            container_name: Name of the container (default: "shahulin")
            model: Deepgram model to use (default: "nova-2")
            diarize: Enable speaker diarization (default: True)
            summarize: Enable summarization (default: True)
            language_detection: Enable language detection (default: True)
            
        Returns:
            dict: Result of the transcription with metadata
        """
        # Record start time for performance tracking
        start_time = time.time()
        
        try:
            # Generate a SAS URL for the blob
            audio_url = self.generate_sas_url(container_name, blob_name)
            
            # Set up the Deepgram API endpoint
            url = "https://api.deepgram.com/v1/listen"
            
            # Set up headers with API key
            headers = {
                "Authorization": f"Token {self.api_key}",
                "Content-Type": "application/json"
            }
            
            # Prepare the request body with URL and options
            payload = {
                "url": audio_url,
                "model": model,
                "smart_format": True,
                "diarize": diarize,
                "detect_language": language_detection,
                "punctuate": True,
                "utterances": True,
                "summarize": summarize
            }
            
            # Send the request
            logger.info(f"Sending request to Deepgram API with URL input for {blob_name}")
            response = requests.post(url, headers=headers, json=payload, timeout=300)
            
            # Calculate processing time
            processing_time = time.time() - start_time
            logger.info(f"Deepgram API request completed in {processing_time:.2f} seconds")
            
            # Check if the request was successful
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Transcription completed successfully for {blob_name}")
                
                # Add metadata to the response
                metadata = {
                    "processing_time": processing_time,
                    "timestamp": datetime.utcnow().isoformat(),
                    "source_blob": blob_name,
                    "source_container": container_name,
                    "transcription_method": "direct_rest_api"
                }
                
                # Return the result with metadata
                return {
                    "result": result,
                    "metadata": metadata,
                    "error": None
                }
            else:
                error_message = f"Deepgram API request failed: {response.status_code} - {response.text}"
                logger.error(error_message)
                return {
                    "result": None,
                    "metadata": {
                        "processing_time": processing_time,
                        "timestamp": datetime.utcnow().isoformat(),
                        "source_blob": blob_name,
                        "source_container": container_name,
                        "transcription_method": "direct_rest_api"
                    },
                    "error": {
                        "name": "DeepgramApiError", 
                        "message": error_message,
                        "status": response.status_code,
                        "response": response.text
                    }
                }
        except Exception as e:
            # Calculate processing time even if there was an error
            processing_time = time.time() - start_time
            
            error_message = f"Error in transcription: {str(e)}"
            logger.error(error_message)
            import traceback
            logger.error(traceback.format_exc())
            
            return {
                "result": None,
                "metadata": {
                    "processing_time": processing_time,
                    "timestamp": datetime.utcnow().isoformat(),
                    "source_blob": blob_name,
                    "source_container": container_name,
                    "transcription_method": "direct_rest_api"
                },
                "error": {
                    "name": "TranscriptionError",
                    "message": error_message
                }
            }
    
    def move_blob(self, blob_name, source_container="shahulin", destination_container="shahulout"):
        """
        Move a blob from one container to another by copying and then optionally deleting the source.
        
        Args:
            blob_name: Name of the blob to move
            source_container: Source container name (default: "shahulin")
            destination_container: Destination container name (default: "shahulout")
            
        Returns:
            dict: Result of the operation including destination URL
        """
        logger.info(f"Moving blob {blob_name} from {source_container} to {destination_container}")
        
        try:
            # Get source blob client
            source_blob_client = self.blob_service_client.get_blob_client(
                container=source_container, 
                blob=blob_name
            )
            
            # Get destination blob client
            dest_blob_client = self.blob_service_client.get_blob_client(
                container=destination_container, 
                blob=blob_name
            )
            
            # Start the copy operation
            dest_blob_client.start_copy_from_url(source_blob_client.url)
            
            # Generate a SAS URL for the destination blob (for convenience)
            destination_sas_url = self.generate_sas_url(destination_container, blob_name)
            
            logger.info(f"Successfully moved {blob_name} to {destination_container}")
            
            return {
                "status": "success",
                "source_container": source_container,
                "destination_container": destination_container,
                "blob_name": blob_name,
                "destination_url": dest_blob_client.url,
                "destination_sas_url": destination_sas_url
            }
        except Exception as e:
            error_message = f"Error moving blob: {str(e)}"
            logger.error(error_message)
            return {
                "status": "error",
                "source_container": source_container,
                "destination_container": destination_container,
                "blob_name": blob_name,
                "error": error_message
            }
    
    def process_file(self, blob_name, container_name="shahulin", destination_container="shahulout"):
        """
        Complete file processing workflow: transcribe and move the file.
        
        Args:
            blob_name: Name of the blob to process
            container_name: Source container name (default: "shahulin")
            destination_container: Destination container name (default: "shahulout")
            
        Returns:
            dict: Complete processing results including transcription and file movement
        """
        # Record overall start time
        start_time = time.time()
        process_id = f"{int(start_time)}_{blob_name}"
        
        logger.info(f"Starting complete processing for {blob_name} (ID: {process_id})")
        
        # Step 1: Transcribe the file
        transcription_result = self.transcribe(blob_name, container_name)
        
        # Track transcription time
        transcription_time = time.time() - start_time
        logger.info(f"Transcription completed in {transcription_time:.2f} seconds")
        
        # Step 2: Move the file to the destination container
        move_start_time = time.time()
        move_result = self.move_blob(blob_name, container_name, destination_container)
        move_time = time.time() - move_start_time
        
        # Calculate total processing time
        total_processing_time = time.time() - start_time
        
        # Combine all results
        result = {
            "process_id": process_id,
            "blob_name": blob_name,
            "source_container": container_name,
            "destination_container": destination_container,
            "transcription": transcription_result,
            "file_movement": move_result,
            "processing_times": {
                "transcription_time": transcription_time,
                "file_movement_time": move_time,
                "total_processing_time": total_processing_time
            },
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success" if transcription_result.get("error") is None else "error"
        }
        
        logger.info(f"Complete processing finished for {blob_name} in {total_processing_time:.2f} seconds")
        return result

# Example usage
if __name__ == "__main__":
    # Create a DirectTranscribe instance
    transcriber = DirectTranscribe()
    
    # Process a file
    result = transcriber.process_file("agricultural_leasing_(ijarah)_normal.mp3")
    
    # Print the result
    print(json.dumps(result, indent=2))