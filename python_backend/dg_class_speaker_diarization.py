"""
Deepgram Use Case 8: Speaker Diarization and Identification
Refactored into a Python class.
"""

import os
import json
import asyncio
import requests

class DgClassSpeakerDiarization:
    def __init__(self, deepgram_api_key, sql_helper=None):
        """
        Initializes the Speaker Diarization class.
        Args:
            deepgram_api_key (str): The Deepgram API key.
            sql_helper (SQLHelper): An instance of the SQLHelper class for database interactions.
        """
        # Using direct API approach instead of SDK
        self.deepgram_api_key = deepgram_api_key
        self.sql_helper = sql_helper

    async def dg_func_transcribe_audio_with_diarization(self, audio_file_path):
        """
        Transcribe audio using Deepgram API with speaker diarization enabled.
        Args:
            audio_file_path (str): Path to the audio file.
        Returns:
            dict: The Deepgram API response.
        """
        try:
            with open(audio_file_path, "rb") as audio:
                # Determine file type from extension
                file_extension = os.path.splitext(audio_file_path)[1].lower().replace('.', '')
                file_type = file_extension if file_extension in ['mp3', 'wav', 'ogg', 'flac', 'mp4', 'm4a'] else 'wav'
                
                # Set up the API URL with query parameters
                api_url = "https://api.deepgram.com/v1/listen"
                params = {
                    "punctuate": "true", 
                    "diarize": "true", 
                    "detect_language": "true",
                    "model": "nova-2", 
                    "smart_format": "true",
                    "utterances": "true"
                }
                
                # Set up headers with API key
                headers = {
                    "Authorization": f"Token {self.deepgram_api_key}",
                    "Content-Type": f"audio/{file_type}"
                }
                
                print(f"Sending audio file {audio_file_path} to Deepgram for diarization...")
                
                # Read the audio file
                audio_data = audio.read()
                
                # Make async request
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.post(api_url, params=params, headers=headers, data=audio_data)
                )
                
                # Check if the request was successful
                if response.status_code != 200:
                    print(f"Deepgram API error: {response.status_code}, {response.text}")
                    return None
                
                # Parse JSON response
                response_json = response.json()
                return response_json
        except Exception as e:
            print(f"Error during transcription with diarization: {e}")
            return None

    def dg_func_extract_speaker_segments(self, response):
        """
        Extracts speaker-segmented transcript and related information.
        Prioritizes utterances if available, then paragraphs.
        """
        if not response or "results" not in response:
            return "", "Unknown", [], "", 0

        results = response.get("results", {})
        channels = results.get("channels", [{}])
        if not channels: return "", "Unknown", [], "", 0
        
        channel = channels[0]
        detected_language = channel.get("detected_language", "Unknown")
        alternatives = channel.get("alternatives", [{}])
        if not alternatives: return "", detected_language, [], "", 0
        
        alternative = alternatives[0]
        raw_transcript = alternative.get("transcript", "")
        
        formatted_transcript_parts = []
        speaker_segments_data = [] # Renamed to avoid conflict with outer scope if any
        unique_speakers = set()

        utterances = results.get("utterances")
        if utterances:
            for utt in utterances:
                speaker = utt.get("speaker", 0) 
                unique_speakers.add(speaker)
                text = utt.get("transcript", "")
                start_time = utt.get("start")
                end_time = utt.get("end")
                duration = utt.get("duration") if utt.get("duration") is not None else (end_time - start_time if start_time is not None and end_time is not None else None)
                confidence = utt.get("confidence")
                words = utt.get("words", [])
                
                formatted_transcript_parts.append(f"Speaker {speaker}: {text}")
                speaker_segments_data.append({
                    "speaker": speaker,
                    "text": text,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration,
                    "confidence": confidence,
                    "word_count": len(words)
                })
        elif "paragraphs" in alternative and alternative.get("paragraphs") and "paragraphs" in alternative["paragraphs"].get("paragraphs", []):
            paragraphs_data = alternative["paragraphs"]["paragraphs"]
            for para in paragraphs_data:
                speaker = para.get("speaker", 0)
                unique_speakers.add(speaker)
                para_text_parts = []
                para_start_time = None
                para_end_time = None
                word_count = 0
                
                for sentence in para.get("sentences", []):
                    para_text_parts.append(sentence.get("text", ""))
                    word_count += len(sentence.get("text", "").split())
                    if para_start_time is None or sentence.get("start", float("inf")) < para_start_time:
                        para_start_time = sentence.get("start")
                    if para_end_time is None or sentence.get("end", float("-inf")) > para_end_time:
                        para_end_time = sentence.get("end")
                
                full_para_text = " ".join(para_text_parts)
                formatted_transcript_parts.append(f"Speaker {speaker}: {full_para_text}")
                duration = para_end_time - para_start_time if para_start_time is not None and para_end_time is not None else None

                speaker_segments_data.append({
                    "speaker": speaker,
                    "text": full_para_text,
                    "start_time": para_start_time,
                    "end_time": para_end_time,
                    "duration": duration,
                    "confidence": para.get("confidence"),
                    "word_count": word_count
                })
        else: 
            formatted_transcript_parts.append(raw_transcript)

        full_formatted_transcript = "\n\n".join(formatted_transcript_parts)
        num_speakers = len(unique_speakers) if unique_speakers else (1 if raw_transcript else 0)
        
        return full_formatted_transcript, detected_language, speaker_segments_data, raw_transcript, num_speakers

    def dg_func_analyze_speaker_contributions(self, speaker_segments_list, num_speakers_detected):
        """
        Analyzes speaker contributions based on segments (talk time, word count).
        """
        if not speaker_segments_list:
            return {"num_speakers": num_speakers_detected, "speaker_talk_time": {}, "speaker_word_count": {}, "speaker_talk_ratio": {}}

        speakers = sorted(list(set(s["speaker"] for s in speaker_segments_list)))
        if not speakers and num_speakers_detected == 1 and speaker_segments_list:
            speakers = [speaker_segments_list[0]["speaker"]]
        elif not speakers and num_speakers_detected > 0:
            speakers = [f"Speaker_{i}" for i in range(num_speakers_detected)]

        speaker_talk_time = {sp: 0.0 for sp in speakers}
        speaker_word_count = {sp: 0 for sp in speakers}

        for segment in speaker_segments_list:
            spk = segment["speaker"]
            if spk not in speakers: 
                speakers.append(spk)
                speaker_talk_time[spk] = 0.0
                speaker_word_count[spk] = 0
            
            if segment.get("duration") is not None:
                speaker_talk_time[spk] += segment["duration"]
            speaker_word_count[spk] += segment.get("word_count", len(segment.get("text", "").split()))

        total_talk_time = sum(speaker_talk_time.values())
        speaker_talk_ratio = {sp: (time / total_talk_time * 100) if total_talk_time > 0 else 0 
                              for sp, time in speaker_talk_time.items()}
        
        if num_speakers_detected == 1 and not speakers and speaker_segments_list:
            single_speaker_id = 0 
            speaker_talk_time = {single_speaker_id: total_talk_time}
            speaker_word_count = {single_speaker_id: sum(s.get("word_count", len(s.get("text", "").split())) for s in speaker_segments_list)}
            speaker_talk_ratio = {single_speaker_id: 100.0 if total_talk_time > 0 else 0.0}
            num_speakers_detected = 1 

        return {
            "num_speakers": num_speakers_detected,
            "speaker_talk_time": speaker_talk_time,
            "speaker_word_count": speaker_word_count,
            "speaker_talk_ratio": speaker_talk_ratio
        }

    async def main(self, dg_response_json_str_str, fileid, local_audio_path=None): # Added dg_response_json_str and local_audio_path for orchestrator compatibility
        """
        Main function for speaker diarization.
        Args:
            dg_response_json_str (str): JSON string of the Deepgram transcription response from the orchestrator.
            fileid (str): The file ID from the orchestrator.
            local_audio_path (str, optional): Path to the audio file (may not be needed if response is pre-transcribed).
        Returns:
            dict: Results including diarized transcript and speaker analysis.
        """
        # Parse JSON string into dictionary if needed
        if dg_response_json_str_str and isinstance(dg_response_json_str_str, str):
            try:
                dg_response_json_str = json.loads(dg_response_json_str_str)
            except json.JSONDecodeError as e:
                print(f"Failed to parse dg_response_json_str_str as JSON: {str(e)}")
                return {"error": f"Invalid JSON: {str(e)}", "fileid": fileid, "status": "Error"}
        else:
            dg_response_json_str = dg_response_json_str_str

        print(f"Starting Speaker Diarization for fileid: {fileid}")
        results_payload = {}
        status = "Error"
        error_message = None

        try:
            if not dg_response_json_str:
                # This case implies the class is expected to do its own transcription if no response is passed.
                # However, the orchestrator pattern is to pass the main transcription.
                # For now, let's assume dg_response_json_str is primary.
                # If audio_file_path is provided and no dg_response_json_str, then transcribe.
                if local_audio_path:
                    print(f"No pre-existing transcription provided for {fileid}, attempting transcription of {local_audio_path}")
                    transcription_response_dict = await self.dg_func_transcribe_audio_with_diarization(local_audio_path)
                else:
                    raise ValueError("No Deepgram response or audio file path provided.")
            else:
                transcription_response_dict = json.loads(dg_response_json_str)

            if not transcription_response_dict:
                error_message = "Transcription with diarization failed or produced no response."
                print(error_message)
                results_payload = {"error": error_message, "fileid": fileid, "status": status}
            else:
                # Step 2: Extract speaker segments and transcript
                formatted_transcript, detected_language, speaker_segments, raw_transcript, num_speakers_detected = self.dg_func_extract_speaker_segments(transcription_response_dict)

                # Step 3: Analyze speaker contributions
                speaker_analysis = self.dg_func_analyze_speaker_contributions(speaker_segments, num_speakers_detected)
                status = "Success"
                results_payload = {
                    "fileid": fileid,
                    "audio_file_path": local_audio_path, # Logged for reference
                    "detected_language": detected_language,
                    "raw_transcript": raw_transcript,
                    "formatted_diarized_transcript": formatted_transcript,
                    "speaker_segments": speaker_segments,
                    "speaker_analysis_summary": speaker_analysis,
                    "status": status
                }

        except Exception as e_main:
            error_message = f"Error in DgClassSpeakerDiarization main: {e_main}"
            print(error_message)
            status = "Error"
            results_payload = {"error": error_message, "fileid": fileid, "status": status}

        # Step 4: Log results to SQL
        if self.sql_helper:
            try:
                fileid_val = fileid # Already available
                # Extract values for SP based on the structure of results_payload and DDL for DG_LogSpeakerDiarization
                num_speakers_val = results_payload.get("speaker_analysis_summary", {}).get("num_speakers") if status == "Success" else None
                speaker_segments_json_val = json.dumps(results_payload.get("speaker_segments")) if status == "Success" else None
                full_transcript_snippet_val = results_payload.get("formatted_diarized_transcript", "")[:1000] if status == "Success" else results_payload.get("raw_transcript", "")[:1000]
                
                # Ensure error_message is correctly passed if status is Error
                current_error_message = results_payload.get("error") if status == "Error" else None

                params = (
                    fileid_val,
                    num_speakers_val,
                    speaker_segments_json_val,
                    full_transcript_snippet_val,
                    status, # This is the overall status of this class's processing
                    current_error_message
                )
                self.sql_helper.execute_sp("DG_LogSpeakerDiarization", params)
                print(f"Speaker diarization results for fileid: {fileid_val} logged to SQL with status: {status}.")
            except Exception as e_sql:
                sql_error_msg = f"Error logging speaker diarization results to SQL for fileid {fileid}: {e_sql}"
                print(sql_error_msg)
                # If logging fails, we should reflect this. The main status might still be Success from analysis point of view.
                # For now, we just print the error. The orchestrator might log a generic class failure.
                if results_payload.get("status") == "Success": # If analysis was success but logging failed
                    results_payload["status_sql_logging"] = "Error_SQLLogging"
                    results_payload["error_sql_logging"] = sql_error_msg
        else:
            print(f"SQL Helper not available. Skipping SQL logging for fileid: {fileid}.")
        
        print(f"Speaker diarization processing completed for fileid: {fileid}. Final status: {results_payload.get('status')}")
        return results_payload

# Example usage (for testing purposes)
async def example_run():
    # This example key is from user's pasted_content.txt, likely expired/invalid for actual calls.
    DEEPGRAM_API_KEY = "c8b36dcd6ca2e6b521a1b07dcd3425a0b5f01a18" 
    
    # Create a dummy sql_helper for testing if actual SQL connection is not desired for this example
    class DummySQLHelper:
        def execute_sp(self, sp_name, params):
            print(f"[DummySQLHelper] Called {sp_name} with params: {params}")

    # dummy_sql_helper = DummySQLHelper()
    # For a real test, sql_helper should be initialized as in the orchestrator.
    # For now, this example will run without a real sql_helper, so SQL logging will be skipped.
    dummy_sql_helper = None 

    diarizer = DgClassSpeakerDiarization(deepgram_api_key=DEEPGRAM_API_KEY, sql_helper=dummy_sql_helper)
    example_fileid = "testfile001"

    # Create a dummy Deepgram response JSON string for testing the extraction and SP call logic
    # without making a live API call, which might fail with an invalid key.
    dummy_dg_response_str = json.dumps({
        "results": {
            "channels": [{
                "detected_language": "en",
                "alternatives": [{
                    "transcript": "Hello Speaker 0. Hello Speaker 1.",
                    "paragraphs": {
                        "transcript": "Hello Speaker 0. Hello Speaker 1.",
                        "paragraphs": [
                            {
                                "speaker": 0, 
                                "sentences": [{"text": "Hello Speaker 0.", "start": 0.5, "end": 1.5}], 
                                "num_words": 3, 
                                "start": 0.5, 
                                "end": 1.5
                            },
                            {
                                "speaker": 1, 
                                "sentences": [{"text": "Hello Speaker 1.", "start": 1.8, "end": 2.8}], 
                                "num_words": 3, 
                                "start": 1.8, 
                                "end": 2.8
                            }
                        ]
                    }
                }]
            }],
            "utterances": [
                {"speaker": 0, "transcript": "Hello Speaker 0.", "start": 0.5, "end": 1.5, "duration": 1.0, "confidence": 0.9, "words": []},
                {"speaker": 1, "transcript": "Hello Speaker 1.", "start": 1.8, "end": 2.8, "duration": 1.0, "confidence": 0.9, "words": []}
            ]
        }
    })

    # results = await diarizer.main(dg_response_json_str=dummy_dg_response_str, fileid=example_fileid)
    # To test with a live call (if key is valid and dummy_audio.wav exists):
    dummy_audio_path = "/home/ubuntu/dummy_audio.wav"
    if not os.path.exists(dummy_audio_path):
        print(f"Dummy audio file {dummy_audio_path} not found for example_run. Creating a placeholder.")
        try:
            import wave
            with wave.open(dummy_audio_path, "w") as wf:
                wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(16000)
                wf.writeframes(b"\x00\x00" * 16000 * 2) # 2 secs silence
            print(f"Created dummy audio file: {dummy_audio_path}")
        except Exception as e_wave:
            print(f"Could not create dummy audio file: {e_wave}. Exiting example.")
            return
    # For live call test, pass local_audio_path and set dg_response_json_str to None
    results = await diarizer.main(dg_response_json_str=None, fileid=example_fileid, local_audio_path=dummy_audio_path)

    if results and results.get("status") == "Success":
        print("\n--- Example Run Results (Speaker Diarization) ---")
        analysis_summary = results.get("speaker_analysis_summary", {})
        print(f"Detected Language: {results.get('detected_language')}")
        print(f"Number of Speakers Detected: {analysis_summary.get('num_speakers')}")
        if analysis_summary.get("speaker_talk_time"):
            print("Speaker Talk Time (s):")
            for speaker, time_val in analysis_summary['speaker_talk_time'].items():
                print(f"  - Speaker {speaker}: {time_val:.2f}s ({analysis_summary['speaker_talk_ratio'].get(speaker, 0):.1f}%)")
    else:
        print("\n--- Example Run Failed (Speaker Diarization) ---")
        print(results)

if __name__ == "__main__":
    # Ensure nest_asyncio is applied if running this standalone for testing asyncio in Jupyter-like environments
    # import nest_asyncio
    # nest_asyncio.apply()
    asyncio.run(example_run())


