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

# Global variable to store direct transcription results
direct_transcription_results = {}

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
            try:
                azure_storage_service.download_blob(filename, local_path)
                logger.info(f"Downloaded {filename} to {local_path}")
            except Exception as e:
                logger.error(f"Failed to download file from Azure Blob Storage: {str(e)}")
                return jsonify({"error": f"Azure storage error: {str(e)}"}), 500
            
            # Validate audio file
            file_size = os.path.getsize(local_path)
            logger.info(f"Audio file size: {file_size} bytes")
            
            if file_size == 0:
                logger.error(f"Audio file is empty: {filename}")
                return jsonify({"error": f"Audio file is empty: {filename}"}), 400
            
            if file_size < 100:  # Suspicious file size for audio
                logger.warning(f"Suspiciously small audio file: {filename} ({file_size} bytes)")
                
            # Log first few bytes to help with debugging
            with open(local_path, "rb") as f:
                header = f.read(24)  # Read more bytes for better format detection
                logger.info(f"File header: {header.hex()}")
                
                # Check file format based on extension
                file_extension = os.path.splitext(filename)[1].lower()
                valid_format = True
                
                if file_extension == '.wav':
                    # Verify WAV header (should start with "RIFF" and contain "WAVE")
                    if header[:4] != b'RIFF' or header[8:12] != b'WAVE':
                        valid_format = False
                        logger.warning(f"File {filename} does not appear to be a valid WAV file")
                elif file_extension == '.mp3':
                    # Verify MP3 header (should start with ID3 or have a sync word)
                    if not (header[:3] == b'ID3' or header[0:2] == b'\xFF\xFB' or header[0:2] == b'\xFF\xF3' or header[0:2] == b'\xFF\xFA'):
                        valid_format = False
                        logger.warning(f"File {filename} does not appear to be a valid MP3 file")
                    else:
                        logger.info(f"MP3 header verification passed")
                
                if not valid_format:
                    logger.warning(f"Proceeding with potentially invalid file format for {filename}")
            
            # Choose processing method based on configuration
            transcription_method = os.environ.get("DEEPGRAM_TRANSCRIPTION_METHOD", "rest_api")
            
            if transcription_method == "direct":
                # Use the direct REST API implementation
                try:
                    from azure_deepgram_transcribe import process_audio_file as direct_process_audio
                    logger.info(f"Using direct Deepgram REST API implementation for {filename}")
                    
                    # Send the file name directly to the direct processor - it will handle the Azure Storage interaction
                    # This is a key change - we're not using the locally downloaded file for direct processing
                    logger.info(f"Passing blob name {filename} directly to the Azure Deepgram transcribe module")
                    result = direct_process_audio(filename, fileid)
                    
                    # Log the result structure for debugging
                    if isinstance(result, dict):
                        logger.info(f"Direct process result keys: {', '.join(result.keys())}")
                        if "transcription" in result and isinstance(result["transcription"], dict):
                            logger.info(f"Transcription data type: {type(result['transcription'])}")
                            if isinstance(result["transcription"], dict):
                                logger.info(f"Transcription object keys: {', '.join(result['transcription'].keys())}")
                    
                except Exception as e:
                    logger.error(f"Error using direct API implementation: {str(e)}")
                    logger.error(f"Detailed error: {traceback.format_exc()}")
                    # Fall back to regular implementation
                    logger.info(f"Falling back to standard implementation")
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    result = loop.run_until_complete(deepgram_service.process_audio_file(local_path, fileid))
            else:
                # Use the standard implementation (SDK or REST API)
                logger.info(f"Using standard implementation ({transcription_method}) for {filename}")
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
            
            # Move to destination container and get the destination URL
            destination_url = azure_storage_service.copy_blob_to_destination(filename)
            logger.info(f"Moved {filename} to destination container")
            
            # Update the destination path in the database
            try:
                from azure_sql_service import AzureSQLService
                sql_service = AzureSQLService()
                conn = sql_service._get_connection()
                cursor = conn.cursor()
                
                cursor.execute("""
                    UPDATE rdt_assets 
                    SET destination_path = %s
                    WHERE fileid = %s
                """, (
                    destination_url,
                    fileid
                ))
                
                conn.commit()
                cursor.close()
                conn.close()
                logger.info(f"Updated destination path in database for file {fileid}")
            except Exception as e:
                logger.error(f"Error updating destination path: {str(e)}")
            
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
        
@app.route('/setup/stored-procedures', methods=['GET'])
def setup_stored_procedures():
    """Set up missing stored procedures"""
    try:
        from create_missing_sp import main as create_sp_main
        create_sp_main()
        return jsonify({"status": "success", "message": "Stored procedures created successfully"})
    except Exception as e:
        logger.error(f"Error creating stored procedures: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/config/transcription-method', methods=['GET', 'POST'])
def configure_transcription_method():
    """Get or set the transcription method (SDK, REST API or direct)"""
    try:
        # Check current setting
        current_method = os.environ.get("DEEPGRAM_TRANSCRIPTION_METHOD", "rest_api")
        
        # Handle POST request to update the method
        if request.method == 'POST':
            data = request.json
            new_method = data.get('method', '').lower()
            
            # Validate the method
            if new_method not in ['sdk', 'rest_api', 'direct', 'shortcut']:
                return jsonify({"error": "Invalid transcription method. Use 'sdk', 'rest_api', 'direct', or 'shortcut'"}), 400
            
            # Update the environment variable
            os.environ["DEEPGRAM_TRANSCRIPTION_METHOD"] = new_method
            logger.info(f"Transcription method changed from '{current_method}' to '{new_method}'")
            
            return jsonify({
                "status": "success", 
                "message": f"Transcription method set to {new_method}",
                "previous_method": current_method,
                "current_method": new_method
            })
        
        # Handle GET request to get current method
        else:
            return jsonify({
                "current_method": current_method,
                "available_methods": ["sdk", "rest_api", "direct", "shortcut"]
            })
            
    except Exception as e:
        logger.error(f"Error configuring transcription method: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/debug/direct-transcriptions', methods=['GET'])
def get_direct_transcriptions():
    """Get the raw results from test_direct_transcription calls"""
    try:
        # Get stored direct transcription results
        global direct_transcription_results
        results = {}
        
        # Since the Workflow was restarted, let's process a file directly to get a result
        if request.args.get('test_file'):
            filename = request.args.get('test_file')
            from test_direct_transcription import test_direct_transcription
            result = test_direct_transcription(blob_name=filename)
            results[filename] = {
                'timestamp': datetime.now().isoformat(),
                'result': result
            }
            
        # Return all stored results
        return jsonify({
            "status": "success",
            "count": len(results),
            "results": results
        })
    except Exception as e:
        logger.error(f"Error retrieving direct transcription results: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port)