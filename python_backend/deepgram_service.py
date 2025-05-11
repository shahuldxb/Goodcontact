import requests
import os
import json
import logging
import asyncio
import time
from datetime import datetime
import traceback

# Import the Deepgram classes from the attached assets
from dg_class_sentiment_analysis import DgClassSentimentAnalysis
from dg_class_language_detection import DgClassLanguageDetection
from dg_class_call_summarization import DgClassCallSummarization
from dg_class_forbidden_phrases import DgClassForbiddenPhrases
from dg_class_topic_detection import DgClassTopicDetection
from dg_class_speaker_diarization import DgClassSpeakerDiarization

class DeepgramService:
    def __init__(self):
        """Initialize the Deepgram Service with all analysis classes"""
        self.logger = logging.getLogger(__name__)
        
        # Deepgram API key
        self.deepgram_api_key = os.environ.get("DEEPGRAM_API_KEY", "d6290865c35bddd50928c5d26983769682fca987")
        
        # Initialize API access
        try:
            self.api_url = "https://api.deepgram.com/v1/listen"
            
            # Initialize analysis classes
            self.sentiment_analysis = DgClassSentimentAnalysis(self.deepgram_api_key)
            self.language_detection = DgClassLanguageDetection(self.deepgram_api_key)
            self.call_summarization = DgClassCallSummarization(self.deepgram_api_key)
            self.forbidden_phrases = DgClassForbiddenPhrases(self.deepgram_api_key)
            self.topic_detection = DgClassTopicDetection(self.deepgram_api_key)
            self.speaker_diarization = DgClassSpeakerDiarization(self.deepgram_api_key)
            
            self.logger.info("Deepgram Service initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing Deepgram Service: {str(e)}")
            traceback.print_exc()
            raise
    
    async def transcribe_audio(self, audio_file_path):
        """Transcribe audio using Deepgram API"""
        try:
            # Validate file exists
            if not os.path.exists(audio_file_path):
                self.logger.error(f"File does not exist: {audio_file_path}")
                return {"result": None, "error": {"name": "FileNotFoundError", "message": f"File does not exist: {audio_file_path}", "status": 404}}
                
            # Get file size for logging
            file_size = os.path.getsize(audio_file_path)
            self.logger.info(f"Audio file size: {file_size} bytes")
            
            # Determine file type from extension
            file_extension = os.path.splitext(audio_file_path)[1].lower().replace('.', '')
            supported_types = ['mp3', 'wav', 'ogg', 'flac', 'mp4', 'm4a']
            file_type = file_extension if file_extension in supported_types else 'wav'
            
            self.logger.info(f"File extension: {file_extension}, using mimetype: audio/{file_type}")
            
            # Set up the API URL with query parameters
            params = {
                "model": "nova-2",
                "smart_format": "true",
                "diarize": "true",
                "punctuate": "true",
                "detect_language": "true",
                "summarize": "true"
            }
            
            # Set up headers with API key
            headers = {
                "Authorization": f"Token {self.deepgram_api_key}",
                "Content-Type": f"audio/{file_type}"
            }
            
            self.logger.info(f"Sending audio file {audio_file_path} with mimetype audio/{file_type} to Deepgram for transcription...")
            
            # Read the audio file and verify it contains data
            with open(audio_file_path, 'rb') as audio_file:
                audio_data = audio_file.read()
                
                if len(audio_data) == 0:
                    self.logger.error(f"Audio file is empty: {audio_file_path}")
                    return {"result": None, "error": {"name": "EmptyFileError", "message": f"Audio file is empty: {audio_file_path}", "status": 400}}
                
                # Log the first few bytes for diagnostics (hex format)
                self.logger.info(f"First 20 bytes of audio file: {audio_data[:20].hex()}")
                
                # Make async request with error handling
                try:
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        None,
                        lambda: requests.post(self.api_url, params=params, headers=headers, data=audio_data)
                    )
                    
                    # Check if the request was successful
                    if response.status_code != 200:
                        error_message = f"Deepgram API error: {response.status_code}, {response.text}"
                        self.logger.error(error_message)
                        return {"result": None, "error": {"name": "DeepgramApiError", "message": response.text, "status": response.status_code}}
                    
                    # Parse JSON response
                    response_json = response.json()
                    
                    # Log the full response
                    self.logger.info(f"DEEPGRAM RAW RESPONSE: {json.dumps(response_json)}")
                    
                    # Return a properly structured response
                    return {"result": response_json, "error": None}
                    
                except Exception as e:
                    error_message = f"Error making request to Deepgram API: {str(e)}"
                    self.logger.error(error_message)
                    traceback.print_exc()
                    return {"result": None, "error": {"name": "RequestError", "message": error_message, "status": 500}}
        except Exception as e:
            self.logger.error(f"Error during transcription: {str(e)}")
            traceback.print_exc()
            raise
    
    async def process_audio_file(self, audio_file_path, fileid=None):
        """Process an audio file with all analysis types"""
        try:
            start_time = time.time()
            
            # Generate a file ID if not provided
            if not fileid:
                fileid = f"file_{int(time.time())}_{os.path.basename(audio_file_path).split('.')[0]}"
            
            self.logger.info(f"Processing audio file: {audio_file_path} with ID: {fileid}")
            
            # Perform transcription
            transcription_response = await self.transcribe_audio(audio_file_path)
            transcription_json_str = json.dumps(transcription_response)
            
            # Extract the transcript text using a robust approach
            transcript_text = ""
            extraction_path = "None"
            
            # Log the structure of the response to help debug
            self.logger.info(f"RESPONSE KEYS: {transcription_response.keys()}")
            
            # Method 1: Standard path in Deepgram schema (results.channels[].alternatives[].transcript)
            if 'results' in transcription_response and 'channels' in transcription_response['results']:
                self.logger.info("Found 'results.channels' path")
                for channel in transcription_response['results']['channels']:
                    if 'alternatives' in channel and len(channel['alternatives']) > 0:
                        if 'transcript' in channel['alternatives'][0]:
                            transcript_text += channel['alternatives'][0]['transcript']
                            extraction_path = "results.channels[].alternatives[].transcript"
            
            # Method 2: Alternative path in some Deepgram responses (results.alternatives[].transcript)
            if not transcript_text and 'results' in transcription_response and 'alternatives' in transcription_response['results']:
                self.logger.info("Found 'results.alternatives' path")
                for alt in transcription_response['results']['alternatives']:
                    if 'transcript' in alt:
                        transcript_text += alt['transcript']
                        extraction_path = "results.alternatives[].transcript"
            
            # Method 3: Alternative path in some Deepgram responses (results.utterances[].transcript)
            if not transcript_text and 'results' in transcription_response and 'utterances' in transcription_response['results']:
                self.logger.info("Found 'results.utterances' path")
                for utt in transcription_response['results']['utterances']:
                    if 'transcript' in utt:
                        transcript_text += utt['transcript'] + " "
                        extraction_path = "results.utterances[].transcript"
            
            # Method 4: Alternative path in some Deepgram responses (results.paragraphs.paragraphs[].text)
            if not transcript_text and 'results' in transcription_response and 'paragraphs' in transcription_response['results']:
                self.logger.info("Found 'results.paragraphs' path")
                if 'paragraphs' in transcription_response['results']['paragraphs']:
                    for para in transcription_response['results']['paragraphs']['paragraphs']:
                        if 'text' in para:
                            transcript_text += para['text'] + " "
                            extraction_path = "results.paragraphs.paragraphs[].text"
            
            # Method 5: Direct flattened structure (channels[].alternatives[].transcript)
            if not transcript_text and 'channels' in transcription_response:
                self.logger.info("Found 'channels' path")
                for channel in transcription_response['channels']:
                    if 'alternatives' in channel and len(channel['alternatives']) > 0:
                        if 'transcript' in channel['alternatives'][0]:
                            transcript_text += channel['alternatives'][0]['transcript']
                            extraction_path = "channels[].alternatives[].transcript"
            
            # Method 6: Direct flattened structure (alternatives[].transcript)
            if not transcript_text and 'alternatives' in transcription_response:
                self.logger.info("Found 'alternatives' path")
                for alt in transcription_response['alternatives']:
                    if 'transcript' in alt:
                        transcript_text += alt['transcript']
                        extraction_path = "alternatives[].transcript"
            
            # Method 7: Sometimes the summary may contain useful text if transcript fails
            if not transcript_text and 'results' in transcription_response and 'summary' in transcription_response['results']:
                self.logger.info("Found 'results.summary' path")
                if 'short' in transcription_response['results']['summary']:
                    transcript_text = transcription_response['results']['summary']['short']
                    extraction_path = "results.summary.short"
                elif 'long' in transcription_response['results']['summary']:
                    transcript_text = transcription_response['results']['summary']['long']
                    extraction_path = "results.summary.long"
            
            self.logger.info(f"Extraction path used: {extraction_path}")
            self.logger.info(f"Extracted transcript ({len(transcript_text)} chars): {transcript_text[:100]}...")
            
            # Save transcription to database
            from azure_sql_service import AzureSQLService
            sql_service = AzureSQLService()
            conn = sql_service._get_connection()
            cursor = conn.cursor()
            
            # Extract the detected language using various paths
            detected_language = 'en'  # Default fallback to English
            language_path = 'default'
            
            # Try various paths where the language might be found
            if ('results' in transcription_response and 'metadata' in transcription_response['results'] and 
                'detected_language' in transcription_response['results']['metadata']):
                detected_language = transcription_response['results']['metadata']['detected_language']
                language_path = 'results.metadata.detected_language'
            elif ('metadata' in transcription_response and 
                  'detected_language' in transcription_response['metadata']):
                detected_language = transcription_response['metadata']['detected_language']
                language_path = 'metadata.detected_language'
            elif ('results' in transcription_response and 
                  'language' in transcription_response['results']):
                detected_language = transcription_response['results']['language']
                language_path = 'results.language'
            elif 'language' in transcription_response:
                detected_language = transcription_response['language']
                language_path = 'language'
            
            self.logger.info(f"Extracted language: {detected_language} via path: {language_path}")
            
            # Check if asset already exists
            cursor.execute("SELECT * FROM rdt_assets WHERE fileid = %s", (fileid,))
            existing_asset = cursor.fetchone()
            
            if existing_asset:
                # Update existing asset
                cursor.execute("""
                    UPDATE rdt_assets 
                    SET transcription = %s, 
                        transcription_json = %s, 
                        language_detected = %s,
                        status = 'processing'
                    WHERE fileid = %s
                """, (
                    transcript_text,
                    transcription_json_str,
                    detected_language,
                    fileid
                ))
            else:
                # Create new asset
                cursor.execute("""
                    INSERT INTO rdt_assets 
                    (fileid, filename, source_path, file_size, transcription, transcription_json, language_detected, status) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    fileid,
                    os.path.basename(audio_file_path),
                    audio_file_path,
                    os.path.getsize(audio_file_path),
                    transcript_text,
                    transcription_json_str,
                    detected_language,
                    'processing'
                ))
            
            conn.commit()
            
            # Run all analyses in parallel
            analyses_tasks = [
                self.sentiment_analysis.main(transcription_json_str, fileid),
                self.language_detection.main(transcription_json_str, fileid),
                self.call_summarization.main(transcription_json_str, fileid),
                self.forbidden_phrases.main(transcription_json_str, fileid, audio_file_path),
                self.topic_detection.main(transcription_json_str, fileid, audio_file_path),
                self.speaker_diarization.main(transcription_json_str, fileid, audio_file_path)
            ]
            
            self.logger.info(f"Running all 6 analyses for fileid: {fileid}")
            await asyncio.gather(*analyses_tasks)
            self.logger.info(f"All 6 analyses completed for fileid: {fileid}")
            
            # Update asset status to completed
            processing_time = int((time.time() - start_time) * 1000)  # Convert to milliseconds
            
            cursor.execute("""
                UPDATE rdt_assets 
                SET status = 'completed', 
                    processed_date = %s, 
                    processing_duration = %s 
                WHERE fileid = %s
            """, (
                datetime.now(),
                processing_time,
                fileid
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return {
                "fileid": fileid,
                "status": "completed",
                "processingTime": processing_time
            }
            
        except Exception as e:
            self.logger.error(f"Error processing audio file: {str(e)}")
            traceback.print_exc()
            
            # Update asset status to error
            try:
                conn = sql_service._get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE rdt_assets 
                    SET status = 'error', 
                        error_message = %s 
                    WHERE fileid = %s
                """, (
                    str(e),
                    fileid
                ))
                conn.commit()
                cursor.close()
                conn.close()
            except Exception as sql_e:
                self.logger.error(f"Error updating asset status: {str(sql_e)}")
            
            raise

# Import the Deepgram classes
from dg_class_sentiment_analysis import DgClassSentimentAnalysis
from dg_class_language_detection import DgClassLanguageDetection
from dg_class_call_summarization import DgClassCallSummarization
from dg_class_forbidden_phrases import DgClassForbiddenPhrases
from dg_class_topic_detection import DgClassTopicDetection
from dg_class_speaker_diarization import DgClassSpeakerDiarization