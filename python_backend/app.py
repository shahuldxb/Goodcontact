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
            transcription_method = os.environ.get("DEEPGRAM_TRANSCRIPTION_METHOD", "shortcut")
            
            if transcription_method == "enhanced":
                # Use enhanced transcription with database storage
                try:
                    from transcription_with_storage import transcribe_and_store
                    logger.info(f"Using enhanced transcription with database storage for {filename}")
                    
                    # Generate a SAS URL for the blob
                    sas_url = azure_storage_service.get_sas_url(filename)
                    
                    # Process using enhanced transcription
                    result = transcribe_and_store(
                        file_url=sas_url,
                        fileid=fileid,
                        store_results=True
                    )
                    
                    logger.info(f"Enhanced transcription completed for {filename}")
                    
                except Exception as e:
                    logger.error(f"Error using enhanced transcription: {str(e)}")
                    logger.error(f"Detailed error: {traceback.format_exc()}")
                    # Fall back to regular implementation
                    logger.info(f"Falling back to standard implementation")
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    result = loop.run_until_complete(deepgram_service.process_audio_file(local_path, fileid))
            elif transcription_method == "direct":
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
        
@app.route('/setup/sentence-tables', methods=['GET'])
def setup_sentence_tables():
    """Set up new tables for paragraphs and sentences"""
    try:
        from update_sentence_tables import update_sentence_tables
        result = update_sentence_tables()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error creating sentence tables: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
        
@app.route('/store/transcription-details', methods=['POST'])
def store_transcription_details():
    """Store detailed transcription data including paragraphs and sentences"""
    try:
        data = request.json
        fileid = data.get('fileid')
        transcription_response = data.get('transcription')
        
        if not fileid or not transcription_response:
            return jsonify({"status": "error", "message": "Missing required fields: fileid or transcription"}), 400
            
        from update_sentence_tables import store_transcription_details
        result = store_transcription_details(fileid, transcription_response)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error storing transcription details: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/config/transcription-method', methods=['GET', 'POST'])
def configure_transcription_method():
    """Get or set the transcription method (SDK, REST API or direct)"""
    try:
        # Check current setting
        current_method = os.environ.get("DEEPGRAM_TRANSCRIPTION_METHOD", "shortcut")
        
        # Handle POST request to update the method
        if request.method == 'POST':
            data = request.json
            new_method = data.get('method', '').lower()
            
            # Validate the method
            if new_method not in ['sdk', 'rest_api', 'direct', 'shortcut', 'enhanced']:
                return jsonify({"error": "Invalid transcription method. Use 'sdk', 'rest_api', 'direct', 'shortcut', or 'enhanced'"}), 400
            
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
                "available_methods": ["sdk", "rest_api", "direct", "shortcut", "enhanced"]
            })
            
    except Exception as e:
        logger.error(f"Error configuring transcription method: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/debug/direct-transcription', methods=['GET'])
def get_direct_transcription():
    """Get the raw results from test_direct_transcription calls for Azure blob files"""
    try:
        # Get test file parameter
        filename = request.args.get('test_file')
        
        if not filename:
            return jsonify({
                "status": "error",
                "message": "Please provide a test_file parameter"
            }), 400
            
        # Import the run_test function and the extract_transcript function
        from direct_test import run_test, extract_transcript
        result = run_test(filename)
        
        # Also get the formatted transcript directly
        formatted_transcript = extract_transcript(result)
        
        # Return formatted results
        return jsonify({
            "status": "success",
            "filename": filename,
            "timestamp": datetime.now().isoformat(),
            "formatted_transcript": formatted_transcript,
            "result": result
        })
    except Exception as e:
        logger.error(f"Error retrieving direct transcription results: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
        
@app.route('/debug/direct-transcription-upload', methods=['POST'])
def upload_and_transcribe():
    """Upload a local file and run direct transcription on it"""
    try:
        # Check if the post request has the file part
        if 'file' not in request.files:
            return jsonify({
                "status": "error", 
                "message": "No file part in the request"
            }), 400
            
        file = request.files['file']
        
        # If user submits empty form
        if file.filename == '':
            return jsonify({
                "status": "error", 
                "message": "No file selected"
            }), 400
            
        if file:
            # Create a temporary directory to save the uploaded file
            temp_dir = tempfile.mkdtemp()
            file_path = os.path.join(temp_dir, file.filename)
            
            # Save the file temporarily
            file.save(file_path)
            logger.info(f"Uploaded file saved to {file_path}")
            
            # Get file info for logging
            file_size = os.path.getsize(file_path)
            logger.info(f"Audio file size: {file_size} bytes")
            
            # Get the current transcription method
            transcription_method = os.environ.get("DEEPGRAM_TRANSCRIPTION_METHOD", "shortcut")
            logger.info(f"Using transcription method: {transcription_method}")
            
            # Get additional parameters
            store_results = request.form.get('store_results', 'false').lower() == 'true'
            fileid = request.form.get('fileid', f"local_{int(time.time())}")
            
            # Process the file based on the current transcription method
            try:
                if transcription_method == "enhanced":
                    # Use enhanced transcription with metadata extraction and optional database storage
                    from transcription_with_storage import transcribe_and_store
                    logger.info(f"Using enhanced transcription with storage = {store_results}")
                    result = transcribe_and_store(
                        file_path=file_path,
                        fileid=fileid,
                        store_results=store_results
                    )
                    formatted_transcript = result.get('transcript', '')
                elif transcription_method == "direct" or transcription_method == "shortcut":
                    # For direct/shortcut transcription, we need to use the local file
                    from transcription_methods import transcribe_audio_directly
                    result = transcribe_audio_directly(file_path)
                    from direct_test import extract_transcript
                    formatted_transcript = extract_transcript(result)
                else:
                    # For SDK and REST API methods, use the DeepgramService
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    result = loop.run_until_complete(deepgram_service.process_audio_file(file_path, fileid))
                    from direct_test import extract_transcript
                    formatted_transcript = extract_transcript(result)
                
                # Save the result to file
                from direct_test import OUTPUT_DIR
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = os.path.join(OUTPUT_DIR, f"local_{os.path.splitext(file.filename)[0]}_{timestamp}.json")
                
                # Create output directory if it doesn't exist
                if not os.path.exists(OUTPUT_DIR):
                    os.makedirs(OUTPUT_DIR)
                
                output_data = {
                    "blob_name": file.filename,
                    "timestamp": datetime.now().isoformat(),
                    "execution_time_seconds": 0,  # We don't track this for local uploads
                    "formatted_transcript": formatted_transcript,
                    "result": result
                }
                
                with open(output_file, "w") as f:
                    json.dump(output_data, f, indent=2)
                
                # Clean up the temporary file
                os.remove(file_path)
                os.rmdir(temp_dir)
                
                # Return the result
                return jsonify({
                    "status": "success",
                    "filename": file.filename,
                    "timestamp": datetime.now().isoformat(),
                    "formatted_transcript": formatted_transcript,
                    "result": result
                })
            
            except Exception as e:
                logger.error(f"Error processing uploaded file: {str(e)}")
                traceback.print_exc()
                # Clean up the temporary file
                if os.path.exists(file_path):
                    os.remove(file_path)
                if os.path.exists(temp_dir):
                    os.rmdir(temp_dir)
                return jsonify({"status": "error", "message": f"Processing error: {str(e)}"}), 500
    
    except Exception as e:
        logger.error(f"Error handling file upload: {str(e)}")
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Upload error: {str(e)}"}), 500
        
@app.route('/debug/direct-test-results', methods=['GET'])
def list_direct_test_results():
    """List files containing direct transcription test results"""
    try:
        # Look for results in the direct_test_results directory
        from direct_test import OUTPUT_DIR
        
        # Create directory if it doesn't exist
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            
        # List all JSON files in the directory
        result_files = []
        for filename in os.listdir(OUTPUT_DIR):
            if filename.endswith('.json'):
                file_path = os.path.join(OUTPUT_DIR, filename)
                file_stat = os.stat(file_path)
                result_files.append({
                    'filename': filename,
                    'size': file_stat.st_size,
                    'created': datetime.fromtimestamp(file_stat.st_ctime).isoformat()
                })
                
        return jsonify({
            "status": "success",
            "count": len(result_files),
            "files": result_files
        })
    except Exception as e:
        logger.error(f"Error listing direct test results: {str(e)}")
        return jsonify({"error": str(e)}), 500
        
@app.route('/debug/direct-test-results/<filename>', methods=['GET'])
def get_direct_test_result(filename):
    """Get a specific direct transcription test result file"""
    try:
        from direct_test import OUTPUT_DIR
        file_path = os.path.join(OUTPUT_DIR, filename)
        
        if not os.path.exists(file_path):
            return jsonify({
                "status": "error",
                "message": f"File not found: {filename}"
            }), 404
            
        # Read and return the file contents
        with open(file_path, 'r') as f:
            result = json.load(f)
            
        return jsonify({
            "status": "success",
            "filename": filename,
            "result": result
        })
    except Exception as e:
        logger.error(f"Error retrieving direct test result: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/generate-urls', methods=['GET'])
def generate_urls():
    """Generate both regular blob URL and SAS URL for a file"""
    try:
        # Get filename parameter
        filename = request.args.get('filename')
        
        if not filename:
            return jsonify({
                "status": "error",
                "message": "Please provide a filename parameter"
            }), 400
            
        # Get container parameter (optional)
        container = request.args.get('container', azure_storage_service.source_container)
        
        # Get expiry_hours parameter (optional)
        try:
            expiry_hours = int(request.args.get('expiry_hours', 240))
        except ValueError:
            expiry_hours = 240
            
        # Generate regular blob URL
        blob_url = azure_storage_service.get_blob_url(container, filename)
        
        # Generate SAS URL
        sas_url = azure_storage_service.generate_sas_url(container, filename, expiry_hours)
        
        # Format expiry time for display
        expiry_time = (datetime.now() + timedelta(hours=expiry_hours)).isoformat()
        
        return jsonify({
            "status": "success",
            "filename": filename,
            "container": container,
            "blob_url": blob_url,
            "sas_url": sas_url,
            "expiry_hours": expiry_hours,
            "expiry_time": expiry_time,
            "timestamp": datetime.now().isoformat()
        })
            
    except Exception as e:
        logger.error(f"Error generating URLs: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port)