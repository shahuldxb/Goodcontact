from deepgram import DeepgramClient, PrerecordedOptions
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
        
        # Initialize Deepgram client
        try:
            self.deepgram = DeepgramClient(self.deepgram_api_key)
            
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
            with open(audio_file_path, "rb") as audio:
                source = {"buffer": audio, "mimetype": "audio/mp3"}
                options = PrerecordedOptions(
                    punctuate=True,
                    diarize=True,
                    detect_language=True,
                    model="nova-2",
                    smart_format=True,
                    summarize=True
                )
                
                self.logger.info(f"Sending audio file {audio_file_path} to Deepgram for transcription...")
                response = await self.deepgram.listen.prerecorded.transcribe_file(source, options)
                
                # Log the full response structure
                self.logger.info(f"Deepgram response structure: {json.dumps(response.to_dict(), indent=2)}")
                
                return response
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
            transcription_json_str = json.dumps(transcription_response.to_dict())
            
            # Extract the transcript text
            transcript_text = ""
            if transcription_response.results and transcription_response.results.channels:
                for channel in transcription_response.results.channels:
                    if channel.alternatives and len(channel.alternatives) > 0:
                        transcript_text += channel.alternatives[0].transcript
            
            self.logger.info(f"Extracted transcript: {transcript_text[:100]}...")
            
            # Save transcription to database
            from azure_sql_service import AzureSQLService
            sql_service = AzureSQLService()
            conn = sql_service._get_connection()
            cursor = conn.cursor()
            
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
                    transcription_response.results.metadata.detected_language if transcription_response.results.metadata.detected_language else 'en',
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
                    transcription_response.results.metadata.detected_language if transcription_response.results.metadata.detected_language else 'en',
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