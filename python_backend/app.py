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

# Initialize DirectTranscribeDB with connection parameters
db_transcriber = DirectTranscribeDB(sql_conn_params={
    'server': AZURE_SQL_SERVER,
    'database': AZURE_SQL_DATABASE,
    'user': AZURE_SQL_USER,
    'password': AZURE_SQL_PASSWORD
})

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/health/db', methods=['GET'])
def db_health_check():
    """
    Database health check endpoint to verify connectivity.
    """
    try:
        # Test connection
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
                "message": "Successfully connected to Azure SQL database",
                "query_result": result[0] if result else None,
                "tables_found": table_count,
                "timestamp": datetime.now().isoformat()
            })
        
        return jsonify({
            "status": "error",
            "message": "Failed to connect to Azure SQL database - connection was null",
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
            "processing_time": 0  # We don't track this here
        }
        
        # Store transcription with paragraphs and sentences in database
        logger.info(f"Storing transcription with paragraphs and sentences for {fileid}")
        db_result = db_transcriber.store_transcription_result(processing_result)
        
        if db_result.get("status") == "error":
            logger.error(f"Error storing transcription in database: {db_result.get('message')}")
            # Continue anyway - we'll return the transcription even if DB storage failed
        else:
            logger.info(f"Successfully stored transcription in database: {db_result.get('paragraphs_processed', 0)} paragraphs processed")
        
        # Check if we have paragraphs in the result
        paragraphs_found = 0
        sentences_found = 0
        paragraph_details = []
        
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
        
        # Structure 1: Direct in results
        if "results" in response_data and "paragraphs" in response_data["results"]:
            if "paragraphs" in response_data["results"]["paragraphs"]:
                paragraphs = response_data["results"]["paragraphs"]["paragraphs"]
                paragraphs_found = len(paragraphs)
                logger.info(f"Found {paragraphs_found} paragraphs in structure 1 (direct in results)")
                
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
        
        # Simply log if we didn't find paragraphs
        if not paragraphs_found:
            logger.warning("No paragraphs found in any structure of the response")
            
            # Check for utterances as an alternative structure to log
            if utterances_found:
                logger.info(f"No paragraphs, but found {len(utterances)} utterances in transcription")
                
                # Just log the first few utterances for information purposes
                for i, utterance in enumerate(utterances[:3]):
                    speaker = utterance.get('speaker', 0)
                    text = utterance.get('transcript', '')
                    logger.info(f"Utterance {i}: Speaker {speaker}: {text[:100]}...")
        
        if not paragraphs_found:
            logger.warning("Could not extract or create any paragraphs from the transcription")
        
        logger.info(f"Found {paragraphs_found} paragraphs and {sentences_found} sentences in transcription")
        if paragraph_details:
            logger.info(f"First paragraph: {paragraph_details[0]}")
            
        # No artificial paragraphs - we just log what we found or didn't find
        
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
                "success": db_result.get("status") == "success",
                "paragraphs_processed": db_result.get("paragraphs_processed", 0)
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