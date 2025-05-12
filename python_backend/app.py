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
db_transcriber = DirectTranscribeDB()

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

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
        
        # Store transcription with paragraphs and sentences in database
        logger.info(f"Storing transcription with paragraphs and sentences for {fileid}")
        db_result = db_transcriber.store_transcription_result(
            fileid=fileid,
            blob_name=filename,
            transcription_result=result["result"],
            transcript_text=result["transcript"]
        )
        
        if db_result.get("status") == "error":
            logger.error(f"Error storing transcription in database: {db_result.get('message')}")
            # Continue anyway - we'll return the transcription even if DB storage failed
        else:
            logger.info(f"Successfully stored transcription in database: {db_result.get('paragraphs_processed', 0)} paragraphs processed")
        
        # Extract useful information for response
        response = {
            "success": True,
            "fileid": fileid,
            "filename": filename,
            "transcript_length": len(result["transcript"]),
            "result": result["result"],
            "transcript": result["transcript"],
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