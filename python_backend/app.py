#!/usr/bin/env python3
from flask import Flask, request, jsonify
import os
import logging
import json
import asyncio
from direct_transcribe import DirectTranscribe
from direct_transcribe_db import DirectTranscribeDB
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

# Initialize DirectTranscribe and DirectTranscribeDB
transcriber = DirectTranscribe(DEEPGRAM_API_KEY)

# Azure SQL Server connection parameters
AZURE_SQL_SERVER = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
AZURE_SQL_DATABASE = os.environ.get("AZURE_SQL_DATABASE", "call") 
AZURE_SQL_USER = os.environ.get("AZURE_SQL_USER", "shahul")
AZURE_SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")

# Import the direct SQL test and our enhanced DB classes
from test_direct_sql import test_direct_connection
from direct_sql_connection import DirectSQLConnection
from direct_transcribe_db_enhanced import DirectTranscribeDBEnhanced

# Create a direct SQL connection using our proven working approach
direct_sql = DirectSQLConnection(
    server=AZURE_SQL_SERVER,
    database=AZURE_SQL_DATABASE,
    user=AZURE_SQL_USER,
    password=AZURE_SQL_PASSWORD
)

# Initialize the enhanced DirectTranscribeDB with reliable SQL connection
db_transcriber_enhanced = DirectTranscribeDBEnhanced(
    server=AZURE_SQL_SERVER,
    database=AZURE_SQL_DATABASE,
    user=AZURE_SQL_USER,
    password=AZURE_SQL_PASSWORD
)

# Keep the original DirectTranscribeDB for compatibility
# Note: Using explicit parameters to match the working test_direct_sql.py
db_transcriber = DirectTranscribeDB(sql_conn_params={
    'server': AZURE_SQL_SERVER,
    'database': AZURE_SQL_DATABASE,
    'user': AZURE_SQL_USER,
    'password': AZURE_SQL_PASSWORD,
    'port': '1433',
    'tds_version': '7.3'
})

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/schema/tables', methods=['GET'])
def get_schema_for_tables():
    """
    Get the schema for all RDT tables
    """
    try:
        # Get column names for all three tables
        tables = ['rdt_asset', 'rdt_paragraphs', 'rdt_sentences']
        schema = {}
        
        for table in tables:
            query = f"""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table}' 
            ORDER BY ORDINAL_POSITION
            """
            
            result = direct_sql.execute_query(query)
            
            if result:
                column_names = [col[0] for col in result]
                schema[table] = column_names
        
        return jsonify({
            "status": "ok",
            "tables": schema,
            "timestamp": datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error getting schema: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error getting schema: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/schema/rdt_asset', methods=['GET'])
def rdt_asset_schema():
    """
    Get the schema of the rdt_asset table
    """
    try:
        query = """
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'rdt_asset' 
        ORDER BY ORDINAL_POSITION
        """
        
        result = direct_sql.execute_query(query)
        
        if result:
            column_names = [col[0] for col in result]
            return jsonify({
                "status": "ok",
                "table": "rdt_asset",
                "columns": column_names,
                "timestamp": datetime.now().isoformat()
            })
        
        return jsonify({
            "status": "error",
            "message": "Could not retrieve schema for rdt_asset table",
            "timestamp": datetime.now().isoformat()
        }), 500
    
    except Exception as e:
        logger.error(f"Error getting rdt_asset schema: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error getting rdt_asset schema: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/health/db', methods=['GET'])
def db_health_check():
    """
    Database health check endpoint to verify connectivity.
    """
    try:
        # Explicitly show parameters for debugging
        conn_params = {
            'server': AZURE_SQL_SERVER,
            'database': AZURE_SQL_DATABASE,
            'user': AZURE_SQL_USER,
            'password': '******' # Masked for security
        }
        logger.info(f"Attempting to connect to Azure SQL Server with params: {conn_params}")
        
        # Use our reliable DirectSQLConnection
        success, message = direct_sql.test_connection()
        if success:
            logger.info("Direct SQL connection successful")
            
            # Check tables using DirectSQLConnection
            try:
                conn = direct_sql.get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME IN ('rdt_asset', 'rdt_paragraphs', 'rdt_sentences')
                """)
                table_count = cursor.fetchone()[0]
                conn.close()
                
                return jsonify({
                    "status": "ok",
                    "message": "Successfully connected to Azure SQL database using reliable connection",
                    "connection_message": message,
                    "tables_found": table_count,
                    "timestamp": datetime.now().isoformat()
                })
            except Exception as e:
                logger.warning(f"Connected but couldn't check tables: {str(e)}")
                return jsonify({
                    "status": "ok",
                    "message": "Successfully connected to Azure SQL database, but couldn't check tables",
                    "connection_message": message,
                    "timestamp": datetime.now().isoformat()
                })
        
        # Fallback approach - try direct test
        logger.warning("DirectSQLConnection failed, trying test_direct_connection()")
        success2, message2 = test_direct_connection()
        if success2:
            logger.info("test_direct_connection successful")
            return jsonify({
                "status": "ok",
                "message": "Successfully connected to Azure SQL database using test_direct_connection",
                "connection_message": message2,
                "timestamp": datetime.now().isoformat()
            })
            
        # As a last resort, try the original method
        logger.warning("All direct methods failed, trying original db_transcriber._get_connection()")
        try:
            conn = db_transcriber._get_connection()
            if conn:
                cursor = conn.cursor()
                
                # Check if we can execute a simple query
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                
                # Check tables
                cursor.execute("""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME IN ('rdt_asset', 'rdt_paragraphs', 'rdt_sentences')
                """)
                table_count = cursor.fetchone()[0]
                
                conn.close()
                
                return jsonify({
                    "status": "ok",
                    "message": "Successfully connected to Azure SQL database using original method",
                    "query_result": result[0] if result else None,
                    "tables_found": table_count,
                    "timestamp": datetime.now().isoformat()
                })
        except Exception as e:
            logger.error(f"Original connection method failed: {str(e)}")
        
        return jsonify({
            "status": "error",
            "message": "Failed to connect to Azure SQL database - all methods failed",
            "direct_sql_error": message,
            "test_direct_error": message2 if 'message2' in locals() else "Not attempted",
            "timestamp": datetime.now().isoformat()
        }), 500
    
    except Exception as e:
        logger.error(f"DB health check failed: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to connect to Azure SQL database: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/config/transcription-method', methods=['GET'])
def get_transcription_method():
    return jsonify({
        "method": "direct",
        "description": "Uses direct REST API calls to Deepgram with SAS URLs"
    })

@app.route('/direct/transcribe', methods=['POST'])
def direct_transcribe():
    try:
        data = request.json
        
        if not data:
            return jsonify({"success": False, "error": "No data provided"}), 400
        
        # Get filename, fileid, and file_size from request
        filename = data.get('filename')
        fileid = data.get('fileid')
        file_size = data.get('file_size', 0)  # Default to 0 if not provided
        
        if not filename:
            return jsonify({"success": False, "error": "No filename provided"}), 400
        
        if not fileid:
            return jsonify({"success": False, "error": "No fileid provided"}), 400
        
        logger.info(f"Processing file {filename} with ID {fileid} and size {file_size} using direct REST API approach")
        
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
        
        # Call the transcribe_audio method with explicit paragraph and sentence support
        result = transcriber.transcribe_audio(
            sas_url, 
            paragraphs=True,
            punctuate=True,
            smart_format=True,
            diarize=True
        )
        
        if not result["success"]:
            logger.error(f"Transcription failed: {result['error']['message']}")
            return jsonify({
                "success": False, 
                "error": result['error']['message'],
                "fileid": fileid
            }), 400
        
        # Prepare the result format that store_transcription_result expects
        processing_result = {
            "blob_name": filename, 
            "source_container": SOURCE_CONTAINER,
            "destination_container": "shahulout",
            "transcription": {
                "success": result["success"],
                "result": result["result"],
                "transcript": result["transcript"],
                "error": result.get("error")
            },
            "file_movement": {
                "success": True,
                "destination_url": f"https://infolder.blob.core.windows.net/shahulout/{filename}"
            },
            "fileid": fileid,
            "processing_time": 0,  # We don't track this here
            "file_size": file_size  # Add the file size to the processing_result
        }
        
        # Check if we have paragraphs in the result
        paragraphs_found = 0
        sentences_found = 0
        paragraph_details = []
        paragraphs = []  # Initialize paragraphs list
        
        # Analyze Deepgram response to log paragraphs and sentences
        # Note: According to Deepgram's structure, paragraphs may appear directly in 'results'
        response_data = result["result"]
        
        # First check for utterances (which might be an alternative way to get structured content)
        utterances_found = False
        utterances = []
        if "results" in response_data and "utterances" in response_data["results"]:
            utterances = response_data["results"]["utterances"]
            logger.info(f"Found {len(utterances)} utterances in transcription")
            utterances_found = len(utterances) > 0
            
            # Process first few utterances for logging
            for i, utterance in enumerate(utterances[:3]):
                logger.info(f"Utterance {i}: Speaker {utterance.get('speaker', 'unknown')}: {utterance.get('transcript', '')[:100]}...")
        
        # Check for paragraphs in various possible structures
        logger.info("Checking for paragraphs in response structure...")
        
        # Log the response structure to debug where paragraphs might be
        logger.info(f"Response keys: {list(response_data.keys())}")
        if "results" in response_data:
            logger.info(f"Results keys: {list(response_data['results'].keys())}")
            
            # Examine channels structure if present
            if "channels" in response_data["results"]:
                channels = response_data["results"]["channels"]
                logger.info(f"Found {len(channels)} channels in response")
                
                # Log first channel structure
                if channels and len(channels) > 0:
                    logger.info(f"First channel keys: {list(channels[0].keys())}")
                    
                    # Check alternatives if present
                    if "alternatives" in channels[0]:
                        alternatives = channels[0]["alternatives"]
                        logger.info(f"Found {len(alternatives)} alternatives in first channel")
                        
                        # Log first alternative structure
                        if alternatives and len(alternatives) > 0:
                            logger.info(f"First alternative keys: {list(alternatives[0].keys())}")
        
        # Structure 1: Direct in results
        if "results" in response_data and "paragraphs" in response_data["results"]:
            logger.info(f"Paragraphs type: {type(response_data['results']['paragraphs'])}")
            if isinstance(response_data["results"]["paragraphs"], dict) and "paragraphs" in response_data["results"]["paragraphs"]:
                paragraphs = response_data["results"]["paragraphs"]["paragraphs"]
                paragraphs_found = len(paragraphs)
                logger.info(f"Found {paragraphs_found} paragraphs in structure 1 (direct in results)")
            elif isinstance(response_data["results"]["paragraphs"], list):
                paragraphs = response_data["results"]["paragraphs"]
                paragraphs_found = len(paragraphs)
                logger.info(f"Found {paragraphs_found} paragraphs in structure 1 (direct list in results)")
                
                # Process paragraphs...
                for i, para in enumerate(paragraphs[:3]):
                    para_text = para.get("text", "")[:100] + "..." if len(para.get("text", "")) > 100 else para.get("text", "")
                    sentences = para.get("sentences", [])
                    sentences_found += len(sentences)
                    
                    # Get first sentence if available
                    first_sentence = sentences[0].get("text", "") if sentences else "No sentences"
                    
                    paragraph_details.append({
                        "text": para_text,
                        "sentences_count": len(sentences),
                        "first_sentence": first_sentence
                    })
                    
                    logger.info(f"Paragraph {i}: {para_text}")
                    if sentences:
                        logger.info(f"First sentence: {first_sentence}")
        
        # Structure 2: In channels > alternatives
        if not paragraphs_found and "results" in response_data and "channels" in response_data["results"]:
            channels = response_data["results"]["channels"]
            for channel_idx, channel in enumerate(channels):
                if "alternatives" in channel:
                    alternatives = channel["alternatives"]
                    for alt_idx, alternative in enumerate(alternatives):
                        if "paragraphs" in alternative:
                            logger.info(f"Found paragraphs in structure 2 (channel {channel_idx}, alternative {alt_idx})")
                            
                            if "paragraphs" in alternative["paragraphs"]:
                                paragraphs = alternative["paragraphs"]["paragraphs"]
                                paragraphs_found = len(paragraphs)
                                logger.info(f"Found {paragraphs_found} paragraphs in alternative structure")
                                
                                # Process paragraphs...
                                for i, para in enumerate(paragraphs[:3]):
                                    para_text = para.get("text", "")[:100] + "..." if len(para.get("text", "")) > 100 else para.get("text", "")
                                    sentences = para.get("sentences", [])
                                    sentences_found += len(sentences)
                                    
                                    # Get first sentence if available
                                    first_sentence = sentences[0].get("text", "") if sentences else "No sentences"
                                    
                                    paragraph_details.append({
                                        "text": para_text,
                                        "sentences_count": len(sentences),
                                        "first_sentence": first_sentence
                                    })
                                    
                                    logger.info(f"Paragraph {i}: {para_text}")
                                    if sentences:
                                        logger.info(f"First sentence: {first_sentence}")
                                
                                # Break once we've found paragraphs
                                if paragraphs_found:
                                    break
                        
                        # Break once we've found paragraphs
                        if paragraphs_found:
                            break
                
                # Break once we've found paragraphs
                if paragraphs_found:
                    break
        
        # If we didn't find paragraphs, create them from utterances or transcript
        if not paragraphs_found:
            logger.warning("No paragraphs found in any structure of the response")
            
            # Create paragraphs from utterances if available
            if utterances_found:
                logger.info(f"Creating paragraphs from {len(utterances)} utterances")
                
                # Group utterances by speaker to create paragraphs
                paragraphs = []
                current_speaker = None
                current_paragraph = {"text": "", "start": 0, "end": 0, "speaker": "", "sentences": []}
                
                for utterance in utterances:
                    speaker = utterance.get("speaker", "unknown")
                    text = utterance.get("transcript", "").strip()
                    start_time = utterance.get("start", 0)
                    end_time = utterance.get("end", 0)
                    
                    # Start a new paragraph when speaker changes
                    if current_speaker is None:
                        # First utterance
                        current_speaker = speaker
                        current_paragraph = {
                            "text": text,
                            "start": start_time,
                            "end": end_time,
                            "speaker": speaker,
                            "sentences": []
                        }
                    elif current_speaker != speaker:
                        # Speaker changed, save current paragraph and start a new one
                        if current_paragraph["text"]:
                            paragraphs.append(current_paragraph)
                        
                        current_speaker = speaker
                        current_paragraph = {
                            "text": text,
                            "start": start_time,
                            "end": end_time,
                            "speaker": speaker,
                            "sentences": []
                        }
                    else:
                        # Same speaker, append to current paragraph
                        current_paragraph["text"] += " " + text
                        current_paragraph["end"] = end_time
                
                # Add the last paragraph
                if current_paragraph["text"]:
                    paragraphs.append(current_paragraph)
                
                paragraphs_found = len(paragraphs)
                logger.info(f"Created {paragraphs_found} paragraphs from {utterances_found} utterances")
                
                # Now create sentences from paragraphs
                import re
                for paragraph in paragraphs:
                    # Split text by periods, question marks, and exclamation marks
                    text = paragraph["text"]
                    sentence_texts = re.split(r'(?<=[.!?])\s+', text)
                    
                    # Create sentence objects
                    for sentence_text in sentence_texts:
                        if sentence_text.strip():
                            # Approximate timing - we don't have precise timing for sentences
                            sentence = {
                                "text": sentence_text.strip(),
                                "start": paragraph["start"],
                                "end": paragraph["end"]
                            }
                            paragraph["sentences"].append(sentence)
                            sentences_found += 1
                
                logger.info(f"Created {sentences_found} sentences from paragraphs")
            
            # If we still don't have paragraphs and we have the full transcript, create paragraphs by sentence segmentation
            elif not paragraphs_found and "results" in response_data and "channels" in response_data["results"]:
                channels = response_data["results"]["channels"]
                if channels and len(channels) > 0 and "alternatives" in channels[0] and len(channels[0]["alternatives"]) > 0:
                    alternative = channels[0]["alternatives"][0]
                    if "transcript" in alternative:
                        logger.info("Creating paragraphs from full transcript")
                        import re
                        transcript = alternative["transcript"]
                        
                        # Split by periods, question marks, and exclamation marks followed by space
                        sentence_texts = re.split(r'(?<=[.!?])\s+', transcript)
                        
                        # Group sentences into paragraphs (every 3-5 sentences)
                        paragraphs = []
                        paragraph_size = 3  # Sentences per paragraph
                        
                        for i in range(0, len(sentence_texts), paragraph_size):
                            # Get a group of sentences
                            group = sentence_texts[i:i+paragraph_size]
                            if group and any(s.strip() for s in group):
                                # Create paragraph
                                paragraph_text = " ".join(s for s in group if s.strip())
                                paragraph = {
                                    "text": paragraph_text,
                                    "start": 0,  # We don't have timing info
                                    "end": 0,    # We don't have timing info
                                    "sentences": []
                                }
                                
                                # Add individual sentences
                                for sentence_text in group:
                                    if sentence_text.strip():
                                        sentence = {
                                            "text": sentence_text.strip(),
                                            "start": 0,  # We don't have timing info
                                            "end": 0     # We don't have timing info
                                        }
                                        paragraph["sentences"].append(sentence)
                                        sentences_found += 1
                                
                                paragraphs.append(paragraph)
                        
                        paragraphs_found = len(paragraphs)
                        logger.info(f"Created {paragraphs_found} paragraphs with {sentences_found} sentences from transcript")
        
        # Log the final results
        if not paragraphs_found:
            logger.warning("Could not extract or create any paragraphs from the transcription")
        
        logger.info(f"Found {paragraphs_found} paragraphs and {sentences_found} sentences in transcription")
        if paragraph_details:
            logger.info(f"First paragraph: {paragraph_details[0]}")
            
        # Store transcription with paragraphs and sentences using enhanced database connection
        logger.info(f"Storing transcription with paragraphs and sentences for {fileid}")
        
        # Add the paragraphs to the processing result if we found or created any
        if paragraphs_found > 0:
            processing_result["paragraphs"] = paragraphs
            logger.info(f"Added {paragraphs_found} paragraphs to processing_result")
        
        # Use enhanced DB connection first (which we know works reliably)
        enhanced_db_result = db_transcriber_enhanced.store_transcription_result(processing_result)
        
        if enhanced_db_result.get("status") == "error":
            logger.warning(f"Enhanced DB storage failed: {enhanced_db_result.get('message')}. Trying original method...")
            
            # Fallback to original method if enhanced fails
            original_db_result = db_transcriber.store_transcription_result(processing_result)
            
            if original_db_result.get("status") == "error":
                logger.error(f"Error storing transcription in database (both methods failed): {original_db_result.get('message')}")
                # Continue anyway - we'll return the transcription even if DB storage failed
            else:
                logger.info(f"Successfully stored transcription using original method: {original_db_result.get('paragraphs_processed', 0)} paragraphs processed")
        else:
            logger.info(f"Successfully stored transcription using enhanced method: {enhanced_db_result.get('paragraphs_processed', 0)} paragraphs, {enhanced_db_result.get('sentences_processed', 0)} sentences")
        
        # Extract useful information for response
        response = {
            "success": True,
            "fileid": fileid,
            "filename": filename,
            "transcript_length": len(result["transcript"]),
            "result": result["result"],
            "transcript": result["transcript"],
            "paragraphs_found": paragraphs_found,
            "sentences_found": sentences_found,
            "paragraph_details": paragraph_details[:3] if paragraph_details else [],
            "db_storage": {
                "success": enhanced_db_result.get("status") == "success" if "status" in enhanced_db_result else False,
                "paragraphs_processed": enhanced_db_result.get("paragraphs_processed", 0),
                "sentences_processed": enhanced_db_result.get("sentences_processed", 0)
            }
        }
        
        logger.info(f"Successfully transcribed {filename} (length: {len(result['transcript'])} characters)")
        return jsonify(response), 200
        
    except Exception as e:
        logger.exception(f"Error processing direct transcription request: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/direct/speaker-diarization', methods=['POST'])
def speaker_diarization():
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
        
        logger.info(f"Processing speaker diarization for file {filename} with ID {fileid}")
        
        # Generate SAS URL for the blob (same as in direct_transcribe)
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
        
        # Call the transcribe_audio method with diarization options
        result = transcriber.transcribe_audio(
            sas_url, 
            diarize=True,
            utterances=True,
            paragraphs=True
        )
        
        if not result["success"]:
            logger.error(f"Transcription with diarization failed: {result['error']['message']}")
            return jsonify({
                "success": False, 
                "error": result['error']['message'],
                "fileid": fileid
            }), 400
        
        # Process speaker diarization
        diarization_result = extract_speaker_segments(result["result"])
        
        # Extract useful information for response
        response = {
            "success": True,
            "fileid": fileid,
            "filename": filename,
            "transcript_length": len(result["transcript"]),
            "transcript": result["transcript"],
            "diarization": diarization_result
        }
        
        logger.info(f"Successfully processed speaker diarization for {filename}")
        return jsonify(response), 200
        
    except Exception as e:
        logger.exception(f"Error processing speaker diarization request: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

def extract_speaker_segments(transcription_result):
    """
    Extract speaker segments from Deepgram API response
    """
    try:
        if not transcription_result or "results" not in transcription_result:
            return {"success": False, "error": "Invalid transcription result"}
        
        results = transcription_result.get("results", {})
        channels = results.get("channels", [{}])
        if not channels:
            return {"success": False, "error": "No channels found in transcription result"}
        
        channel = channels[0]
        detected_language = channel.get("detected_language", "Unknown")
        
        speaker_segments = []
        unique_speakers = set()
        formatted_transcript = []
        
        # First try to get utterances (preferred for diarization)
        utterances = results.get("utterances", [])
        if utterances:
            for utt in utterances:
                speaker = utt.get("speaker", 0)
                unique_speakers.add(speaker)
                text = utt.get("transcript", "")
                start_time = utt.get("start")
                end_time = utt.get("end")
                
                formatted_transcript.append(f"Speaker {speaker}: {text}")
                speaker_segments.append({
                    "speaker": speaker,
                    "text": text,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": end_time - start_time if start_time is not None and end_time is not None else None
                })
        # Fallback to paragraphs if no utterances
        elif "alternatives" in channel and channel["alternatives"]:
            alternatives = channel["alternatives"]
            if alternatives and "paragraphs" in alternatives[0]:
                paragraphs = alternatives[0]["paragraphs"].get("paragraphs", [])
                for para in paragraphs:
                    speaker = para.get("speaker", 0)
                    unique_speakers.add(speaker)
                    text = ""
                    start_time = None
                    end_time = None
                    
                    for sentence in para.get("sentences", []):
                        text += sentence.get("text", "")
                        if start_time is None or sentence.get("start", float("inf")) < start_time:
                            start_time = sentence.get("start")
                        if end_time is None or sentence.get("end", float("-inf")) > end_time:
                            end_time = sentence.get("end")
                    
                    formatted_transcript.append(f"Speaker {speaker}: {text}")
                    speaker_segments.append({
                        "speaker": speaker,
                        "text": text,
                        "start_time": start_time,
                        "end_time": end_time,
                        "duration": end_time - start_time if start_time is not None and end_time is not None else None
                    })
        
        speaker_count = len(unique_speakers)
        
        # Calculate speaker statistics
        speaker_stats = {}
        for speaker in unique_speakers:
            segments = [s for s in speaker_segments if s["speaker"] == speaker]
            total_duration = sum(s["duration"] for s in segments if s["duration"] is not None)
            word_count = sum(len(s["text"].split()) for s in segments)
            speaker_stats[str(speaker)] = {
                "total_duration": total_duration,
                "segment_count": len(segments),
                "word_count": word_count
            }
        
        return {
            "success": True,
            "speaker_count": speaker_count,
            "speakers": list(unique_speakers),
            "speaker_segments": speaker_segments,
            "formatted_transcript": "\n\n".join(formatted_transcript),
            "speaker_stats": speaker_stats,
            "language": detected_language
        }
        
    except Exception as e:
        logger.exception(f"Error extracting speaker segments: {str(e)}")
        return {"success": False, "error": str(e)}

if __name__ == '__main__':
    # Check if running in production
    is_production = os.environ.get('FLASK_ENV') == 'production'
    
    # Run the app
    app.run(host='0.0.0.0', port=5001, debug=not is_production)