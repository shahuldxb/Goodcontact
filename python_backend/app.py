#!/usr/bin/env python3
from flask import Flask, request, jsonify
import os
import logging
import json
import asyncio
from direct_transcribe import DirectTranscribe
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Constants
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")
AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", 
                                              "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net")
SOURCE_CONTAINER = "shahulin"

# Initialize DirectTranscribe
transcriber = DirectTranscribe(DEEPGRAM_API_KEY)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/direct/transcribe', methods=['POST'])
async def direct_transcribe():
    try:
        data = request.json
        
        if not data:
            return jsonify({"success": False, "error": "No data provided"}), 400
        
        # Get filename and fileid from request
        filename = data.get('filename')
        fileid = data.get('fileid')
        
        if not filename:
            return jsonify({"success": False, "error": "No filename provided"}), 400
        
        if not fileid:
            return jsonify({"success": False, "error": "No fileid provided"}), 400
        
        logger.info(f"Processing file {filename} with ID {fileid} using direct REST API approach")
        
        # Generate SAS URL for the blob
        from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
        from datetime import timedelta
        
        # Extract account info from connection string
        conn_parts = {p.split('=')[0]: p.split('=', 1)[1] for p in AZURE_STORAGE_CONNECTION_STRING.split(';') if '=' in p}
        account_name = conn_parts.get('AccountName')
        account_key = conn_parts.get('AccountKey')
        
        # Check if blob exists first
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
        blob_client = container_client.get_blob_client(filename)
        
        if not blob_client.exists():
            logger.error(f"File {filename} does not exist in container {SOURCE_CONTAINER}")
            return jsonify({
                "success": False, 
                "error": f"File {filename} does not exist in container {SOURCE_CONTAINER}",
                "fileid": fileid
            }), 404
        
        # Calculate expiry time (240 hours)
        expiry = datetime.now() + timedelta(hours=240)
        
        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=SOURCE_CONTAINER,
            blob_name=filename,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry
        )
        
        # Construct full URL
        sas_url = f"https://{account_name}.blob.core.windows.net/{SOURCE_CONTAINER}/{filename}?{sas_token}"
        logger.info(f"Generated SAS URL for {filename} with 240 hour expiry")
        
        # Transcribe the audio using our DirectTranscribe class
        result = await transcriber.transcribe_audio(sas_url)
        
        if not result["success"]:
            logger.error(f"Transcription failed: {result['error']['message']}")
            return jsonify({
                "success": False, 
                "error": result['error']['message'],
                "fileid": fileid
            }), 400
        
        # Extract useful information for response
        response = {
            "success": True,
            "fileid": fileid,
            "filename": filename,
            "transcript_length": len(result["transcript"]),
            "result": result["result"],
            "transcript": result["transcript"]
        }
        
        logger.info(f"Successfully transcribed {filename} (length: {len(result['transcript'])} characters)")
        return jsonify(response), 200
        
    except Exception as e:
        logger.exception(f"Error processing direct transcription request: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    # Check if running in production
    is_production = os.environ.get('FLASK_ENV') == 'production'
    
    # Run the app
    app.run(host='0.0.0.0', port=5001, debug=not is_production)