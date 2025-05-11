import requests
import os
import json
import logging
import asyncio
import time
from datetime import datetime
import traceback
from deepgram import Deepgram

# We'll use the old SDK approach as the new SDK format isn't available in our installation

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
        self.deepgram_api_key = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")
        
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
    
    async def transcribe_audio_rest_api(self, audio_file_path):
        """
        Transcribe audio using Deepgram REST API (original implementation).
        
        Args:
            audio_file_path (str): Path to the local audio file to transcribe.
            
        Returns:
            dict: A result object with the structure {"result": response_json, "error": error_message}
        """
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
            content_type = f"audio/{file_type}"
            
            # Special case for mp3 files - Deepgram expects audio/mpeg, not audio/mp3
            if file_type == "mp3":
                content_type = "audio/mpeg"
                
            self.logger.info(f"Using content type: {content_type}")
            
            headers = {
                "Authorization": f"Token {self.deepgram_api_key}",
                "Content-Type": content_type
            }
            
            self.logger.info(f"Sending audio file {audio_file_path} with mimetype {content_type} to Deepgram for transcription (REST API)...")
            
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
            self.logger.error(f"Error during REST API transcription: {str(e)}")
            traceback.print_exc()
            raise
            
    async def transcribe_audio_sdk(self, audio_file_path):
        """
        Transcribe audio using the official Deepgram SDK.
        
        Args:
            audio_file_path (str): Path to the local audio file to transcribe.
            
        Returns:
            dict: A result object with the structure {"result": response_json, "error": error_message}
        """
        try:
            # Validate file exists
            if not os.path.exists(audio_file_path):
                self.logger.error(f"File does not exist: {audio_file_path}")
                return {"result": None, "error": {"name": "FileNotFoundError", "message": f"File does not exist: {audio_file_path}", "status": 404}}
                
            # Initialize Deepgram client
            deepgram = Deepgram(self.deepgram_api_key)
            
            # Get file size for logging
            file_size = os.path.getsize(audio_file_path)
            self.logger.info(f"Audio file size: {file_size} bytes")
            
            # Determine file type from extension
            file_extension = os.path.splitext(audio_file_path)[1].lower().replace('.', '')
            supported_types = ['mp3', 'wav', 'ogg', 'flac', 'mp4', 'm4a']
            file_type = file_extension if file_extension in supported_types else 'wav'
            
            # Set up content type
            content_type = f"audio/{file_type}"
            
            # Special case for mp3 files
            if file_type == "mp3":
                content_type = "audio/mpeg"
                
            self.logger.info(f"File extension: {file_extension}, using mimetype: {content_type}")
            
            # Configure transcription options - using same options as REST API method
            options = {
                "model": "nova-2",
                "smart_format": True,
                "diarize": True,
                "punctuate": True,
                "detect_language": True,
                "summarize": True
            }
            
            self.logger.info(f"Sending audio file {audio_file_path} with mimetype {content_type} to Deepgram for transcription (SDK)...")
            
            # Read the audio file and verify it contains data
            with open(audio_file_path, 'rb') as audio_file:
                # For debugging, get the first few bytes
                audio_file.seek(0)
                first_bytes = audio_file.read(20).hex()
                self.logger.info(f"First 20 bytes of audio file: {first_bytes}")
                
                # Reset file pointer to beginning
                audio_file.seek(0)
                
                try:
                    # Send to Deepgram using SDK
                    source = {'buffer': audio_file, 'mimetype': content_type}
                    response = await deepgram.transcription.prerecorded(source, options)
                    
                    # Log the full response
                    self.logger.info(f"DEEPGRAM SDK RESPONSE: {json.dumps(response)}")
                    
                    # Return a properly structured response
                    return {"result": response, "error": None}
                    
                except Exception as e:
                    error_message = f"Error in SDK transcription: {str(e)}"
                    self.logger.error(error_message)
                    traceback.print_exc()
                    return {"result": None, "error": {"name": "SDKError", "message": error_message, "status": 500}}
        except Exception as e:
            self.logger.error(f"Error during SDK transcription: {str(e)}")
            traceback.print_exc()
            raise

    def transcribe_with_listen_rest(self, audio_file_path):
        """
        Transcribe audio using Deepgram's REST API with a direct URL approach.
        This method uses the standard REST API but with a SAS URL input.
        
        Args:
            audio_file_path (str): Path to the local audio file to transcribe.
            
        Returns:
            dict: A result object with the structure {"result": response_json, "error": error_message}
        """
        try:
            self.logger.info(f"Using listen.rest-like API for transcription: {audio_file_path}")
            
            # For testing with local files, we need to create a SAS URL from Azure Storage
            from azure_storage_service import AzureStorageService
            storage = AzureStorageService()
            
            # Get the blob name from the file path
            blob_name = os.path.basename(audio_file_path)
            self.logger.info(f"Generating SAS URL for blob: {blob_name}")
            
            # Generate a SAS URL for the file
            audio_url = storage.generate_sas_url("shahulin", blob_name)
            self.logger.info(f"SAS URL generated: {audio_url[:60]}...")
            
            # Set up the Deepgram API endpoint
            url = "https://api.deepgram.com/v1/listen"
            
            # Set up headers with API key
            headers = {
                "Authorization": f"Token {self.deepgram_api_key}",
                "Content-Type": "application/json"
            }
            
            # Prepare the request body with URL and options
            payload = {
                "url": audio_url,
                "model": "nova-2",  # Using a recent model
                "smart_format": True,
                "diarize": True,
                "detect_language": True,
                "punctuate": True,
                "utterances": True,
                "summarize": True
            }
            
            # Send the request
            self.logger.info("Sending request to Deepgram API with URL input...")
            response = requests.post(url, headers=headers, json=payload)
            
            # Check if the request was successful
            if response.status_code == 200:
                result = response.json()
                self.logger.info("URL-based transcription completed successfully")
                return {"result": result, "error": None}
            else:
                error_message = f"Deepgram API request failed: {response.status_code} - {response.text}"
                self.logger.error(error_message)
                return {"result": None, "error": {"name": "ListenRestError", "message": error_message, "status": response.status_code}}
            
        except Exception as e:
            error_message = f"Error in listen.rest transcription: {str(e)}"
            self.logger.error(error_message)
            import traceback
            self.logger.error(traceback.format_exc())
            return {"result": None, "error": {"name": "ListenRestError", "message": error_message, "status": 500}}

    async def transcribe_audio(self, audio_file_path):
        """
        Main transcription method that can use various Deepgram integration approaches.
        Currently supports:
        - listen.rest (newest and recommended for SAS URLs)
        - sdk (original SDK method)
        - rest_api (manual REST calls)
        - direct (direct Azure blob transcription)
        - shortcut (test implementation)
        
        Args:
            audio_file_path (str): Path to the local audio file to transcribe.
            
        Returns:
            dict: A result object with the structure {"result": response_json, "error": error_message}
        """
        # Import required modules
        import os
        
        # Get environment variable that determines which method to use
        # Defaults to 'rest_api' if not specified
        transcription_method = os.environ.get("DEEPGRAM_TRANSCRIPTION_METHOD", "rest_api").lower()
        
        # URL-based REST API method (highest priority)
        if transcription_method == "listen.rest" or transcription_method == "url":
            self.logger.info("Using URL-based REST API method for transcription")
            result = self.transcribe_with_listen_rest(audio_file_path)
            
            # If the method fails, fall back to SDK
            if result["error"] is not None:
                self.logger.warning("URL-based REST API method failed, falling back to SDK")
                return await self.transcribe_audio_sdk(audio_file_path)
            return result
        
        # Shortcut method as fourth option
        elif transcription_method == "shortcut":
            self.logger.info("Using SHORTCUT method for transcription")
            try:
                # Import the shortcut function
                from transcription_methods import transcribe_audio_shortcut
                
                # Use the shortcut method directly
                result = transcribe_audio_shortcut(audio_file_path)
                
                # If it returns a standard result with 'error' key, handle accordingly
                if isinstance(result, dict) and 'error' in result and result['error']:
                    self.logger.warning(f"SHORTCUT method failed: {result['error']}, falling back to SDK")
                    return await self.transcribe_audio_sdk(audio_file_path)
                
                # Wrap the result in our standard format if needed
                if isinstance(result, dict) and 'error' not in result:
                    return {"result": result, "error": None}
                
                return result
                
            except Exception as e:
                self.logger.error(f"Exception in SHORTCUT method: {str(e)}")
                self.logger.warning("SHORTCUT method failed with exception, falling back to SDK")
                return await self.transcribe_audio_sdk(audio_file_path)
                
        elif transcription_method == "sdk":
            self.logger.info("Using SDK method for transcription")
            result = await self.transcribe_audio_sdk(audio_file_path)
            
            # If SDK method fails, fall back to REST API
            if result["error"] is not None:
                self.logger.warning("SDK method failed, falling back to REST API")
                return await self.transcribe_audio_rest_api(audio_file_path)
            return result
        elif transcription_method == "direct":
            self.logger.info("Using DIRECT method for transcription")
            # For direct method, use the shortcut as a fallback
            try:
                # Import direct method
                from azure_deepgram_transcribe import transcribe_azure_audio
                import os
                
                # Extract the blob name from the file path
                blob_name = os.path.basename(audio_file_path)
                
                # Call direct transcription
                result = transcribe_azure_audio(blob_name=blob_name)
                
                # Handle errors
                if isinstance(result, dict) and ('error' in result or result is None):
                    self.logger.warning("DIRECT method failed, trying SHORTCUT")
                    # Try shortcut as fallback
                    from transcription_methods import transcribe_audio_shortcut
                    result = transcribe_audio_shortcut(audio_file_path)
                
                # Wrap the result
                return {"result": result, "error": None}
                
            except Exception as e:
                self.logger.error(f"Exception in DIRECT method: {str(e)}")
                self.logger.warning("Falling back to REST API after DIRECT and SHORTCUT failures")
                return await self.transcribe_audio_rest_api(audio_file_path)
        else:
            # Default to REST API method
            self.logger.info("Using REST API method for transcription")
            result = await self.transcribe_audio_rest_api(audio_file_path)
            
            # If REST API method fails, fall back to SDK
            if result["error"] is not None:
                self.logger.warning("REST API method failed, falling back to SDK")
                return await self.transcribe_audio_sdk(audio_file_path)
            return result
    
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
            
            # Method 0: Check for direct transcript field in shortcut method response
            if 'transcript' in transcription_response:
                transcript_text = transcription_response['transcript']
                extraction_path = "transcript (root level)"
                self.logger.info(f"Found transcript at root level: {transcript_text[:50]}...")
            # Method 0.1: Check if there's a result key containing transcript (from shortcut method)
            elif 'result' in transcription_response and isinstance(transcription_response['result'], dict) and 'transcript' in transcription_response['result']:
                transcript_text = transcription_response['result']['transcript']
                extraction_path = "result.transcript"
                self.logger.info(f"Found transcript in result.transcript: {transcript_text[:50]}...")
            
            # Method 1: Standard path in Deepgram schema (results.channels[].alternatives[].transcript)
            elif 'results' in transcription_response and 'channels' in transcription_response['results']:
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
            detected_language = 'unknown'  # Default fallback to unknown
            language_confidence = 0.0
            language_path = 'default'
            
            # Parse the nested JSON response
            if isinstance(transcription_response, dict) and 'result' in transcription_response:
                result_json = transcription_response['result']
            else:
                result_json = transcription_response
                
            # Try various paths where the language might be found
            if ('results' in result_json and 'channels' in result_json['results'] and 
                result_json['results']['channels'] and len(result_json['results']['channels']) > 0):
                channel = result_json['results']['channels'][0]
                if 'detected_language' in channel:
                    detected_language = channel['detected_language']
                    language_path = 'results.channels[0].detected_language'
                    # Check for language confidence too
                    if 'language_confidence' in channel:
                        language_confidence = channel['language_confidence']
                        self.logger.info(f"Found language confidence: {language_confidence}")
            elif ('results' in result_json and 'metadata' in result_json['results'] and 
                'detected_language' in result_json['results']['metadata']):
                detected_language = result_json['results']['metadata']['detected_language']
                language_path = 'results.metadata.detected_language'
            elif ('metadata' in result_json and 
                  'detected_language' in result_json['metadata']):
                detected_language = result_json['metadata']['detected_language']
                language_path = 'metadata.detected_language'
            elif ('results' in result_json and 
                  'language' in result_json['results']):
                detected_language = result_json['results']['language']
                language_path = 'results.language'
            elif 'language' in result_json:
                detected_language = result_json['language']
                language_path = 'language'
            
            self.logger.info(f"Extracted language: {detected_language} via path: {language_path}")
            
            # Also directly update rdt_language table
            cursor.execute("SELECT * FROM rdt_language WHERE fileid = %s", (fileid,))
            existing_language = cursor.fetchone()
            
            if existing_language:
                cursor.execute("""
                    UPDATE rdt_language
                    SET language = %s,
                        confidence = %s
                    WHERE fileid = %s
                """, (
                    detected_language,
                    language_confidence,
                    fileid
                ))
            else:
                cursor.execute("""
                    INSERT INTO rdt_language
                    (fileid, language, confidence, status)
                    VALUES (%s, %s, %s, %s)
                """, (
                    fileid,
                    detected_language,
                    language_confidence,
                    'completed'
                ))
                
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
                # Ensure all values are present and valid before inserting
                filename = os.path.basename(audio_file_path)
                source_path = audio_file_path
                
                # Get file size or use default if not accessible
                try:
                    file_size = os.path.getsize(audio_file_path) if os.path.exists(audio_file_path) else 1024
                except:
                    self.logger.warning(f"Could not get file size for {audio_file_path}, using default")
                    file_size = 1024  # Default to 1KB if file size can't be determined
                
                # Ensure transcript text is not null
                if not transcript_text:
                    transcript_text = "Transcript unavailable"
                    self.logger.warning("No transcript text extracted, using placeholder")
                
                # Ensure language is not null
                if not detected_language:
                    detected_language = "en"  # Default to English
                    self.logger.warning("No language detected, using default (en)")
                
                # Create new asset with validated data
                cursor.execute("""
                    INSERT INTO rdt_assets 
                    (fileid, filename, source_path, file_size, transcription, transcription_json, language_detected, status,
                     created_dt) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    fileid,
                    filename,
                    source_path,
                    file_size,
                    transcript_text,
                    transcription_json_str,
                    detected_language,
                    'processing',
                    datetime.now()  # Add current timestamp for created_dt
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
                from azure_sql_service import AzureSQLService
                sql_service = AzureSQLService()
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