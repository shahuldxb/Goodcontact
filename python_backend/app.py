from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
import logging
import tempfile
import sys
import time
from datetime import datetime
import traceback

# Add the current directory to the path so we can import the deepgram classes
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the Deepgram classes
from deepgram_service import DeepgramService
from azure_storage_service import AzureStorageService
from azure_sql_service import AzureSQLService

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize services
try:
    deepgram_service = DeepgramService()
    azure_storage_service = AzureStorageService()
    azure_sql_service = AzureSQLService()
    logger.info("All services initialized successfully")
except Exception as e:
    logger.error(f"Error initializing services: {str(e)}")
    traceback.print_exc()

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/process', methods=['POST'])
def process_file():
    """Process a file from Azure Blob Storage using Deepgram"""
    try:
        data = request.json
        filename = data.get('filename')
        fileid = data.get('fileid')
        
        if not filename:
            return jsonify({"error": "No filename provided"}), 400
        
        logger.info(f"Processing file: {filename} with ID: {fileid}")
        
        # Download file from Azure Blob Storage
        temp_dir = tempfile.mkdtemp()
        local_path = os.path.join(temp_dir, filename)
        
        try:
            azure_storage_service.download_blob(filename, local_path)
            logger.info(f"Downloaded {filename} to {local_path}")
            
            # Validate audio file
            file_size = os.path.getsize(local_path)
            logger.info(f"Audio file size: {file_size} bytes")
            
            if file_size == 0:
                logger.error(f"Audio file is empty: {filename}")
                return jsonify({"error": f"Audio file is empty: {filename}"}), 400
                
            # Log first few bytes to help with debugging
            with open(local_path, "rb") as f:
                header = f.read(12)
                logger.info(f"File header: {header.hex()}")
                
                # Check WAV header
                if header[:4] != b'RIFF' or header[8:12] != b'WAVE':
                    logger.warning(f"File {filename} does not appear to be a valid WAV file")
            
            # Process with Deepgram
            # process_audio_file is already async inside deepgram_service
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(deepgram_service.process_audio_file(local_path, fileid))
            
            # Extract important details for logging
            transcript_length = 0
            has_error = False
            error_message = None
            
            if isinstance(result, dict):
                if "transcription" in result and isinstance(result["transcription"], dict):
                    if "error" in result["transcription"] and result["transcription"]["error"]:
                        has_error = True
                        error_message = result["transcription"]["error"].get("message", "Unknown error")
                
            logger.info(f"Deepgram processing complete: {'ERROR: ' + error_message if has_error else 'SUCCESS'}")
            
            # Move to destination container
            azure_storage_service.copy_blob_to_destination(filename)
            logger.info(f"Moved {filename} to destination container")
            
            return jsonify({"status": "success", "result": result})
            
        except Exception as e:
            logger.error(f"Error processing file {filename}: {str(e)}")
            return jsonify({"error": str(e)}), 500
            
        finally:
            # Clean up temporary file
            if os.path.exists(local_path):
                os.remove(local_path)
                logger.info(f"Cleaned up temporary file: {local_path}")
    
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/files/source', methods=['GET'])
def get_source_files():
    """Get files from source container"""
    try:
        files = azure_storage_service.list_source_blobs()
        return jsonify({"files": files})
    except Exception as e:
        logger.error(f"Error listing source files: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/files/processed', methods=['GET'])
def get_processed_files():
    """Get files from destination container"""
    try:
        files = azure_storage_service.list_destination_blobs()
        return jsonify({"files": files})
    except Exception as e:
        logger.error(f"Error listing processed files: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/analysis/<fileid>', methods=['GET'])
def get_analysis(fileid):
    """Get analysis results for a file"""
    try:
        results = azure_sql_service.get_analysis_results(fileid)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error getting analysis results: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get statistics"""
    try:
        stats = azure_sql_service.get_stats()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/stats/sentiment', methods=['GET'])
def get_sentiment_stats():
    """Get sentiment statistics"""
    try:
        stats = azure_sql_service.get_sentiment_stats()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting sentiment stats: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/stats/topics', methods=['GET'])
def get_topic_stats():
    """Get topic statistics"""
    try:
        stats = azure_sql_service.get_topic_stats()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting topic stats: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port)