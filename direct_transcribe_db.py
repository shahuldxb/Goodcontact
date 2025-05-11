#!/usr/bin/env python3
"""
DirectTranscribeDB Class
This class combines the DirectTranscribe functionality with database operations
for the Call Center Analytics platform.

Key features:
1. Uses direct REST API calls (no SDK)
2. Uses SAS URLs for direct transcription (no download)
3. Database integration with transaction support
4. Robust error handling
5. File existence verification before processing
6. Full performance metrics recording
7. Exactly follows the database schema requirements

Usage:
    transcriber_db = DirectTranscribeDB(deepgram_api_key, db_connection_string)
    result = await transcriber_db.process_audio_file(blob_name, fileid)
    
    if result["success"]:
        print(f"Successfully processed file {blob_name}")
    else:
        print(f"Error processing file {blob_name}: {result['error']}")
"""

import os
import json
import time
import logging
import asyncio
import requests
import traceback
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta

# Azure Storage
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

# Database
import pyodbc

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DirectTranscribeDB:
    """
    DirectTranscribeDB class for transcription with database integration
    """
    
    def __init__(self, deepgram_api_key: str, db_connection_string: str):
        """
        Initialize the DirectTranscribeDB class
        
        Args:
            deepgram_api_key (str): The Deepgram API key
            db_connection_string (str): The database connection string
        """
        self.deepgram_api_key = deepgram_api_key
        self.db_connection_string = db_connection_string
        self.api_endpoint = "https://api.deepgram.com/v1/listen"
        
        # Azure Storage connection
        self.azure_storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
        self.source_container = "shahulin"
        self.destination_container = "shahulout"
        
    async def process_audio_file(self, blob_name: str, fileid: str) -> Dict[str, Any]:
        """
        Process an audio file end-to-end: check if exists, transcribe, and save to database
        
        Args:
            blob_name (str): The name of the blob to process
            fileid (str): The unique file ID for database operations
            
        Returns:
            Dict with keys:
                success (bool): Whether the processing was successful
                result (Dict): The processing result (if successful)
                error (str): Error message (if unsuccessful)
        """
        start_time = time.time()
        processing_stages = []
        
        try:
            # Stage 1: Check if file exists in Azure Storage
            logger.info(f"Checking if file {blob_name} exists in container {self.source_container}")
            processing_stages.append(("check_file_exists", time.time()))
            
            file_exists, file_size = await self._check_blob_exists(blob_name)
            
            if not file_exists:
                logger.error(f"File {blob_name} does not exist in container {self.source_container}")
                return {
                    "success": False,
                    "error": f"File {blob_name} does not exist in container {self.source_container}",
                    "processing_stages": processing_stages
                }
            
            logger.info(f"File {blob_name} exists in container {self.source_container} (size: {file_size} bytes)")
            processing_stages.append(("file_exists_verified", time.time()))
            
            # Stage 2: Generate SAS URL for the file
            logger.info(f"Generating SAS URL for {blob_name}")
            processing_stages.append(("generate_sas_url", time.time()))
            
            sas_url = await self._generate_sas_url(blob_name)
            
            if not sas_url:
                logger.error(f"Failed to generate SAS URL for {blob_name}")
                return {
                    "success": False,
                    "error": f"Failed to generate SAS URL for {blob_name}",
                    "processing_stages": processing_stages
                }
            
            logger.info(f"Generated SAS URL for {blob_name}")
            processing_stages.append(("sas_url_generated", time.time()))
            
            # Stage 3: Transcribe the audio using Deepgram REST API
            logger.info(f"Transcribing {blob_name} with Deepgram REST API")
            processing_stages.append(("transcription_start", time.time()))
            
            transcription_result = await self._transcribe_audio(sas_url)
            
            if not transcription_result["success"]:
                logger.error(f"Transcription failed: {transcription_result['error']['message']}")
                return {
                    "success": False,
                    "error": f"Transcription failed: {transcription_result['error']['message']}",
                    "processing_stages": processing_stages
                }
            
            logger.info(f"Transcription successful (length: {len(transcription_result['transcript'])} characters)")
            processing_stages.append(("transcription_complete", time.time()))
            
            # Stage 4: Save the transcription to the database
            logger.info(f"Saving transcription to database for file {blob_name}")
            processing_stages.append(("database_save_start", time.time()))
            
            db_result = await self._save_to_database(
                fileid=fileid,
                blob_name=blob_name, 
                file_size=file_size,
                transcription_json=transcription_result["result"],
                transcript_text=transcription_result["transcript"]
            )
            
            if not db_result["success"]:
                logger.error(f"Database save failed: {db_result['error']}")
                return {
                    "success": False,
                    "error": f"Database save failed: {db_result['error']}",
                    "processing_stages": processing_stages
                }
            
            logger.info(f"Successfully saved transcription to database for file {blob_name}")
            processing_stages.append(("database_save_complete", time.time()))
            
            # Calculate duration and return success
            end_time = time.time()
            total_duration = end_time - start_time
            
            return {
                "success": True,
                "fileid": fileid,
                "blob_name": blob_name,
                "transcript_length": len(transcription_result["transcript"]),
                "processing_duration": total_duration,
                "processing_stages": processing_stages,
                "database_records": db_result["records_affected"]
            }
            
        except Exception as e:
            logger.exception(f"Error processing {blob_name}: {str(e)}")
            return {
                "success": False,
                "error": f"Error processing {blob_name}: {str(e)}",
                "processing_stages": processing_stages
            }
    
    async def _check_blob_exists(self, blob_name: str) -> Tuple[bool, int]:
        """
        Check if a blob exists in the source container
        
        Args:
            blob_name (str): The name of the blob to check
            
        Returns:
            Tuple[bool, int]: (exists, size)
        """
        try:
            # Connect to blob service
            blob_service_client = BlobServiceClient.from_connection_string(self.azure_storage_connection_string)
            
            # Get container client
            container_client = blob_service_client.get_container_client(self.source_container)
            
            # Get blob client
            blob_client = container_client.get_blob_client(blob_name)
            
            # Check if blob exists
            if blob_client.exists():
                # Get blob properties
                properties = blob_client.get_blob_properties()
                return True, properties.size
            else:
                return False, 0
        except Exception as e:
            logger.error(f"Error checking if blob exists: {str(e)}")
            return False, 0
    
    async def _generate_sas_url(self, blob_name: str, expiry_hours: int = 240) -> Optional[str]:
        """
        Generate a SAS URL for the blob
        
        Args:
            blob_name (str): The name of the blob
            expiry_hours (int): The number of hours until the SAS URL expires
            
        Returns:
            Optional[str]: The SAS URL or None if an error occurred
        """
        try:
            # Extract account info from connection string
            conn_parts = {p.split('=')[0]: p.split('=', 1)[1] for p in self.azure_storage_connection_string.split(';') if '=' in p}
            account_name = conn_parts.get('AccountName')
            account_key = conn_parts.get('AccountKey')
            
            if not account_name or not account_key:
                logger.error("Failed to extract account info from connection string")
                return None
            
            # Calculate expiry time
            expiry = datetime.utcnow() + timedelta(hours=expiry_hours)
            
            # Generate SAS token
            sas_token = generate_blob_sas(
                account_name=account_name,
                container_name=self.source_container,
                blob_name=blob_name,
                account_key=account_key,
                permission=BlobSasPermissions(read=True),
                expiry=expiry
            )
            
            # Construct full URL
            url = f"https://{account_name}.blob.core.windows.net/{self.source_container}/{blob_name}?{sas_token}"
            return url
        except Exception as e:
            logger.error(f"Error generating SAS URL: {str(e)}")
            return None
    
    async def _transcribe_audio(self, audio_url: str) -> Dict[str, Any]:
        """
        Transcribe audio using Deepgram REST API
        
        Args:
            audio_url (str): The SAS URL for the audio file
            
        Returns:
            Dict with keys:
                success (bool): Whether the transcription was successful
                result (Dict): The Deepgram API response (if successful)
                error (Dict): Error details (if unsuccessful)
                transcript (str): The extracted transcript (if successful)
        """
        # Set up request headers
        headers = {
            "Authorization": f"Token {self.deepgram_api_key}",
            "Content-Type": "application/json"
        }
        
        # Default parameters for Deepgram
        payload = {
            "url": audio_url,
            "model": "nova-3",
            "smart_format": True,
            "diarize": True,
            "punctuate": True,
            "utterances": True,
            "paragraphs": True,
            "detect_language": True
        }
        
        try:
            # Send the request to Deepgram (async compatible)
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: requests.post(self.api_endpoint, headers=headers, json=payload, timeout=300)
            )
            
            # Check if the request was successful
            if response.status_code == 200:
                result = response.json()
                
                # Extract transcript to verify content
                transcript = self._extract_transcript(result)
                
                # Verify that we have content
                if not transcript:
                    logger.warning("Transcription succeeded but no transcript content found")
                    return {
                        "success": False,
                        "error": {
                            "message": "Transcription succeeded but no transcript content found",
                            "status": response.status_code
                        },
                        "result": result,
                        "transcript": ""
                    }
                
                # Return success response
                return {
                    "success": True,
                    "result": result,
                    "transcript": transcript,
                    "error": None
                }
            else:
                # Return error response
                return {
                    "success": False,
                    "error": {
                        "message": response.text,
                        "status": response.status_code
                    },
                    "result": None,
                    "transcript": ""
                }
                
        except Exception as e:
            # Return error response
            return {
                "success": False,
                "error": {
                    "message": str(e),
                    "status": None
                },
                "result": None,
                "transcript": ""
            }
    
    def _extract_transcript(self, result: Dict[str, Any]) -> str:
        """
        Extract transcript from Deepgram API response
        
        Args:
            result (Dict): The Deepgram API response
            
        Returns:
            str: The extracted transcript
        """
        try:
            if "results" in result and "channels" in result["results"]:
                channels = result["results"]["channels"]
                if channels and "alternatives" in channels[0]:
                    alternatives = channels[0]["alternatives"]
                    if alternatives and "transcript" in alternatives[0]:
                        return alternatives[0]["transcript"]
            return ""
        except Exception as e:
            logger.error(f"Error extracting transcript: {str(e)}")
            return ""
    
    async def _save_to_database(self, fileid: str, blob_name: str, file_size: int, 
                              transcription_json: Dict[str, Any], transcript_text: str) -> Dict[str, Any]:
        """
        Save the transcription to the database
        
        Args:
            fileid (str): The unique file ID
            blob_name (str): The name of the blob
            file_size (int): The size of the file in bytes
            transcription_json (Dict): The complete Deepgram API response
            transcript_text (str): The extracted transcript text
            
        Returns:
            Dict with keys:
                success (bool): Whether the database operation was successful
                records_affected (int): The number of records affected
                error (str): Error message (if unsuccessful)
        """
        conn = None
        cursor = None
        records_affected = 0
        
        try:
            # Connect to the database
            conn = pyodbc.connect(self.db_connection_string)
            cursor = conn.cursor()
            
            # Start transaction
            conn.autocommit = False
            
            # Step 1: Insert audio metadata to rds_assets table
            logger.info(f"Inserting audio metadata for {fileid}")
            
            # Calculate dates
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Extract duration from transcription_json
            duration = transcription_json.get("metadata", {}).get("duration", 0)
            
            # Execute stored procedure for audio metadata
            cursor.execute(
                "EXEC RDS_InsertAudioMetadata @fileid=?, @filename=?, @filesize=?, @duration=?, @transcription=?, @upload_date=?, @processed_date=?",
                fileid, blob_name, file_size, duration, transcript_text, now, now
            )
            records_affected += cursor.rowcount
            
            # Step 2: Split transcript into paragraphs
            paragraphs = []
            if "results" in transcription_json and "paragraphs" in transcription_json["results"]:
                paragraphs_data = transcription_json["results"].get("paragraphs", {}).get("paragraphs", [])
                for p in paragraphs_data:
                    if "text" in p and "start" in p and "end" in p:
                        paragraphs.append({
                            "text": p["text"],
                            "start": p["start"],
                            "end": p["end"]
                        })
            
            # Insert paragraphs if available
            if paragraphs:
                logger.info(f"Inserting {len(paragraphs)} paragraphs for {fileid}")
                for p in paragraphs:
                    cursor.execute(
                        "EXEC RDS_InsertParagraph @fileid=?, @paragraph_text=?, @start_time=?, @end_time=?",
                        fileid, p["text"], p["start"], p["end"]
                    )
                    records_affected += cursor.rowcount
            
            # Step 3: Split transcript into sentences
            sentences = []
            if "results" in transcription_json and "utterances" in transcription_json["results"]:
                utterances_data = transcription_json["results"].get("utterances", [])
                for u in utterances_data:
                    if "transcript" in u and "start" in u and "end" in u and "speaker" in u:
                        sentences.append({
                            "text": u["transcript"],
                            "start": u["start"],
                            "end": u["end"],
                            "speaker": u["speaker"]
                        })
            
            # Insert sentences if available
            if sentences:
                logger.info(f"Inserting {len(sentences)} sentences for {fileid}")
                for s in sentences:
                    cursor.execute(
                        "EXEC RDS_InsertSentence @fileid=?, @sentence_text=?, @start_time=?, @end_time=?, @speaker=?",
                        fileid, s["text"], s["start"], s["end"], s["speaker"]
                    )
                    records_affected += cursor.rowcount
            
            # Commit transaction
            conn.commit()
            
            return {
                "success": True,
                "records_affected": records_affected,
                "error": None
            }
            
        except Exception as e:
            # Roll back transaction if an error occurs
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            
            # Log error details
            error_message = str(e)
            logger.exception(f"Database error: {error_message}")
            
            return {
                "success": False,
                "records_affected": 0,
                "error": error_message
            }
            
        finally:
            # Close cursor and connection
            if cursor:
                cursor.close()
            if conn:
                conn.close()

# Example usage
if __name__ == "__main__":
    DB_CONNECTION_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=callcenter1.database.windows.net;Database=call;Uid=shahul;Pwd=apple123!@#;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"
    
    async def example():
        transcriber_db = DirectTranscribeDB(DEEPGRAM_API_KEY, DB_CONNECTION_STRING)
        
        # List available files
        blob_service_client = BlobServiceClient.from_connection_string(transcriber_db.azure_storage_connection_string)
        container_client = blob_service_client.get_container_client(transcriber_db.source_container)
        
        audio_files = []
        for blob in container_client.list_blobs():
            if blob.name.lower().endswith(('.mp3', '.wav')):
                audio_files.append(blob.name)
                if len(audio_files) >= 3:
                    break
        
        if not audio_files:
            print("No audio files found in container")
            return
        
        # Select a file for testing
        test_file = audio_files[0]
        fileid = f"test_{int(time.time())}"
        
        print(f"Processing file: {test_file} (fileid: {fileid})")
        result = await transcriber_db.process_audio_file(test_file, fileid)
        
        if result["success"]:
            print(f"Successfully processed file {test_file}")
            print(f"Total processing time: {result['processing_duration']:.2f} seconds")
            print(f"Records affected: {result['database_records']}")
            
            # Print the processing stages
            print("\nProcessing stages:")
            last_time = result['processing_stages'][0][1]
            for stage, stage_time in result['processing_stages']:
                time_diff = stage_time - last_time
                print(f"  {stage}: {time_diff:.4f} seconds")
                last_time = stage_time
        else:
            print(f"Error processing file {test_file}: {result['error']}")
    
    asyncio.run(example())