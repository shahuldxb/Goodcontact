"""
Deepgram Use Case 16: Language Detection and Translation
Refactored into a Python class from deepgram_use_case_16_language_detection.ipynb.

This class performs language detection on audio transcriptions and can translate text.
It uses Deepgram for primary language detection during transcription and can use
other libraries like langdetect for finer-grained analysis and googletrans for translation.
"""

import os
import json
import asyncio
import re
from deepgram import Deepgram

# Attempt to import NLTK, langdetect, googletrans, iso639
try:
    import nltk
    nltk.data.find("tokenizers/punkt.zip") # For sentence tokenization if needed by other libs
except LookupError:
    print("NLTK resource (punkt) not found. Attempting to download...")
    try: nltk.download("punkt", quiet=True); print("NLTK punkt downloaded.")
    except Exception as e: print(f"Failed to download NLTK punkt: {e}.")
except ImportError:
    print("NLTK library not found. Please install it: pip install nltk")

try:
    from langdetect import detect, DetectorFactory
    DetectorFactory.seed = 0 # For reproducibility as in notebook
except ImportError:
    print("langdetect library not found. Please install it: pip install langdetect")
    DetectorFactory = None # Placeholder if import fails

try:
    from googletrans import Translator
except ImportError:
    print("googletrans library not found. Please install it: pip install googletrans==4.0.0-rc1")
    Translator = None # Placeholder

try:
    from iso639 import languages as iso_languages_db
except ImportError:
    print("iso639-lang library not found. Please install it: pip install iso639-lang")
    iso_languages_db = None # Placeholder

class DgClassLanguageDetection:
    def __init__(self, deepgram_api_key, sql_helper=None):
        self.deepgram_api_key = deepgram_api_key # Store API key for direct API calls
        self.sql_helper = sql_helper
        self._ensure_nltk_punkt()

    def _ensure_nltk_punkt(self):
        try:
            nltk.data.find("tokenizers/punkt.zip")
        except LookupError:
            print("NLTK resource (punkt) not found again. Attempting to download...")
            try: nltk.download("punkt", quiet=True); print("NLTK punkt downloaded successfully.")
            except Exception as e: print(f"Failed to download NLTK punkt: {e}. Please ensure it is available.")
        except Exception as e:
            print(f"An error occurred checking NLTK punkt: {e}")

    async def _transcribe_audio(self, audio_source_info):
        try:
            if "file_path" in audio_source_info:
                audio_file_path = audio_source_info["file_path"]
                if not os.path.exists(audio_file_path):
                    print(f"Audio file not found: {audio_file_path}")
                    return None
                with open(audio_file_path, "rb") as audio:
                    source = {"buffer": audio, "mimetype": audio_source_info.get("mimetype", "audio/wav")}
            elif "url" in audio_source_info:
                source = {"url": audio_source_info["url"]}
            else:
                print("Invalid audio_source_info for transcription.")
                return None

            options = PrerecordedOptions(
                model="nova-2", smart_format=True, punctuate=True, diarize=True,
                utterances=True, paragraphs=True, detect_language=True # Crucial for this class
            )
            print(f"Sending audio to Deepgram for transcription with language detection (Language Detection class)...")
            # Updated to use listen.rest as per earlier fixes in orchestrator
            if "buffer" in source:
                response = await self.deepgram_client.listen.rest.v("1").transcribe_file(source, options)
            else:
                response = await self.deepgram_client.listen.rest.v("1").transcribe_url(source, options)
            return response.to_json(indent=None)
        except Exception as e:
            print(f"Error during Deepgram transcription (Language Detection): {e}")
            return None

    def _get_language_name_from_code(self, lang_code):
        if not iso_languages_db:
            return lang_code.upper() if lang_code else "Unknown"
        try:
            return iso_languages_db.get(part1=lang_code).name
        except:
            try: return iso_languages_db.get(part2b=lang_code).name
            except: 
                try: return iso_languages_db.get(part2t=lang_code).name
                except: return lang_code.upper() if lang_code else "Unknown"

    def _extract_main_language_and_transcript(self, dg_response_json_str):
        full_transcript = ""
        detected_language_code = "Unknown"
        language_confidence = 0.0
        speaker_segments_text = []
        try:
            # Parse JSON string to Python dict
            print(f"Extracting language and transcript from response (first 100 chars): {dg_response_json_str[:100]}...")
            
            # Handle case where dg_response_json_str might already be a dict
            if isinstance(dg_response_json_str, dict):
                response = dg_response_json_str
            else:
                try:
                    response = json.loads(dg_response_json_str)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    return full_transcript, detected_language_code, language_confidence, speaker_segments_text
            
            if not response:
                print("Empty response after JSON parsing")
                return full_transcript, detected_language_code, language_confidence, speaker_segments_text
                
            # Print response keys for debugging
            print(f"Response keys: {response.keys() if isinstance(response, dict) else 'Not a dict'}")
            
            # Enhanced extraction - Try multiple possible paths
            
            # Path 1: Standard Deepgram structure (results.channels)
            if "results" in response and "channels" in response["results"] and response["results"]["channels"]:
                print("Using results.channels extraction path")
                results = response["results"]
                channel = results["channels"][0] if results["channels"] else {}
                
                # Extract language info
                if "detected_language" in channel:
                    detected_language_code = channel["detected_language"]
                if "language_confidence" in channel:
                    language_confidence = channel["language_confidence"]
                
                # Extract transcript
                if "alternatives" in channel and channel["alternatives"] and "transcript" in channel["alternatives"][0]:
                    full_transcript = channel["alternatives"][0]["transcript"]
                
                # Extract speaker segments from utterances
                if "utterances" in results and results["utterances"]:
                    for utt in results["utterances"]:
                        speaker_segments_text.append({
                            "speaker": utt.get("speaker", 0),
                            "text": utt.get("transcript", "")
                        })
                # Or from paragraphs
                elif ("alternatives" in channel and channel["alternatives"] and 
                      "paragraphs" in channel["alternatives"][0] and 
                      "paragraphs" in channel["alternatives"][0]["paragraphs"]):
                    
                    paragraphs_data = channel["alternatives"][0]["paragraphs"]
                    for para in paragraphs_data["paragraphs"]:
                        speaker = para.get("speaker", "Unknown")
                        text_parts = [sentence.get("text", "") for sentence in para.get("sentences", [])]
                        speaker_segments_text.append({"speaker": speaker, "text": " ".join(text_parts)})
            
            # Path 2: Alternative structure with direct results.metadata
            elif "results" in response and "metadata" in response["results"] and "detected_language" in response["results"]["metadata"]:
                print("Using results.metadata extraction path")
                detected_language_code = response["results"]["metadata"]["detected_language"]
                
                # Try to find transcript in alternative locations
                if "alternatives" in response["results"] and response["results"]["alternatives"]:
                    full_transcript = response["results"]["alternatives"][0].get("transcript", "")
                elif "transcript" in response["results"]:
                    full_transcript = response["results"]["transcript"]
            
            # Path 3: Flattened structure with metadata at top level
            elif "metadata" in response and "detected_language" in response["metadata"]:
                print("Using top-level metadata extraction path")
                detected_language_code = response["metadata"]["detected_language"]
                
                # Try to find transcript in alternative locations
                if "transcript" in response:
                    full_transcript = response["transcript"]
                elif "alternatives" in response and response["alternatives"]:
                    full_transcript = response["alternatives"][0].get("transcript", "")
            
            # Path 4: Direct language field
            elif "language" in response:
                print("Using direct language field extraction path")
                detected_language_code = response["language"]
                
                # Try to find transcript
                if "transcript" in response:
                    full_transcript = response["transcript"]
            
            # Path 5: Last resort - just look for any transcript field
            if not full_transcript and "transcript" in response:
                print("Using direct transcript field extraction path")
                full_transcript = response["transcript"]
            
            # If we have transcript but no speaker segments, create a default one
            if full_transcript and not speaker_segments_text:
                speaker_segments_text.append({"speaker": "Unknown", "text": full_transcript})
                
            print(f"Extracted language: {detected_language_code} (confidence: {language_confidence})")
            print(f"Transcript length: {len(full_transcript)} chars")
            print(f"Speaker segments: {len(speaker_segments_text)}")

        except Exception as e:
            print(f"Error extracting transcript and language: {e}")
            import traceback
            traceback.print_exc()
            
        return full_transcript, detected_language_code, language_confidence, speaker_segments_text

    def _detect_language_in_text_segments(self, text, min_segment_length=50):
        if not DetectorFactory or not text:
            return []
        sentences = re.split(r"[.!?\n]+\s*", text)
        current_segment_text = ""
        text_segments_for_detection = []
        for sentence in sentences:
            if not sentence.strip(): continue
            if len(current_segment_text) + len(sentence) < min_segment_length and current_segment_text:
                current_segment_text += ". " + sentence
            else:
                if current_segment_text: text_segments_for_detection.append(current_segment_text)
                current_segment_text = sentence
        if current_segment_text: text_segments_for_detection.append(current_segment_text)

        detected_segment_languages = []
        for i, seg_text in enumerate(text_segments_for_detection):
            try:
                lang_code = detect(seg_text)
                lang_name = self._get_language_name_from_code(lang_code)
                detected_segment_languages.append({"id": i, "text_snippet": seg_text[:100]+"...", "lang_code": lang_code, "lang_name": lang_name})
            except Exception as e:
                detected_segment_languages.append({"id": i, "text_snippet": seg_text[:100]+"...", "lang_code": "error", "lang_name": f"Error: {e}"})
        return detected_segment_languages

    def _translate_text_google(self, text, target_lang_code="en"):
        if not Translator or not text:
            return {"translated_text": text if not Translator else "", "source_lang_code": "", "error": "Translator not available or no text"}
        try:
            translator_instance = Translator()
            translation_result = translator_instance.translate(text, dest=target_lang_code)
            return {
                "translated_text": translation_result.text,
                "source_lang_code": translation_result.src,
                "error": None
            }
        except Exception as e:
            print(f"Error during translation: {e}")
            return {"translated_text": "", "source_lang_code": "", "error": str(e)}

    # MODIFIED _log_sql_error method
    def _log_sql_error(self, fileid, error_message, dg_detected_lang_code_at_error=None, target_translation_lang_at_error=None, status="Error"):
        print(f"SQL logging error for FileID {fileid} (Language Detection): {error_message}")
        if self.sql_helper:
            try:
                self.sql_helper.execute_sp("DG_LogLanguageDetection", (
                    fileid,
                    dg_detected_lang_code_at_error, # DgDetectedLanguageCode
                    None,  # DgDetectedLanguageName (can be derived from code if needed, or kept None for error)
                    None,  # DgLanguageConfidence
                    None,  # FullTranscriptTranslated
                    target_translation_lang_at_error, # TranslationTargetLanguage
                    str(error_message)[:1000], # ErrorMessage (original error)
                    None,  # TextSegmentLanguagesJson
                    None,  # TranslatedSpeakerSegmentsJson
                    status # Status
                ))
            except Exception as e_sql_err:
                print(f"Further SQL error while logging initial Language Detection error for FileID {fileid}: {e_sql_err}")

    async def main(self, dg_response_json_str=None, fileid=None, audio_source_info=None, target_translation_lang="en"):
        print(f"Starting Language Detection & Translation for FileID: {fileid}")
        
        # Support either direct JSON response or audio file path
        if not dg_response_json_str and audio_source_info:
            print(f"No JSON response provided. Attempting to transcribe from audio source.")
            dg_response_json_str = await self._transcribe_audio(audio_source_info)
        
        dg_detected_lang_code = None # Initialize for broader scope

        if not dg_response_json_str:
            error_msg = "No transcription data available: both dg_response_json_str and audio_source_info are invalid"
            print(error_msg)
            # MODIFIED call to _log_sql_error
            if self.sql_helper: self._log_sql_error(fileid, error_msg, None, target_translation_lang)
            return {"status": "Error", "fileid": fileid, "error": error_msg}

        full_transcript, dg_detected_lang_code, dg_lang_confidence, speaker_segments_text = self._extract_main_language_and_transcript(dg_response_json_str)
        dg_detected_lang_name = self._get_language_name_from_code(dg_detected_lang_code)

        if not full_transcript:
            # MODIFIED call to _log_sql_error
            if self.sql_helper: self._log_sql_error(fileid, "No transcript extracted", dg_detected_lang_code, target_translation_lang)
            return {"status": "Error", "fileid": fileid, "error": "No transcript extracted"}

        text_segment_languages = self._detect_language_in_text_segments(full_transcript)
        
        translated_full_transcript_info = None
        if dg_detected_lang_code.lower() != target_translation_lang.lower():
            translated_full_transcript_info = self._translate_text_google(full_transcript, target_translation_lang)
        else:
            translated_full_transcript_info = {"translated_text": full_transcript, "source_lang_code": dg_detected_lang_code, "error": "No translation needed"}

        translated_speaker_segments = []
        for seg in speaker_segments_text:
            seg_lang_code_guess = dg_detected_lang_code
            try: seg_lang_code_guess = detect(seg["text"]) if DetectorFactory and seg["text"] else dg_detected_lang_code
            except: pass

            if seg_lang_code_guess.lower() != target_translation_lang.lower() and seg["text"]:
                translation = self._translate_text_google(seg["text"], target_translation_lang)
                translated_speaker_segments.append({
                    "speaker": seg["speaker"],
                    "original_text": seg["text"],
                    "translated_text": translation["translated_text"],
                    "original_lang_code": seg_lang_code_guess,
                    "translation_error": translation["error"]
                })
            else:
                 translated_speaker_segments.append({
                    "speaker": seg["speaker"],
                    "original_text": seg["text"],
                    "translated_text": seg["text"],
                    "original_lang_code": seg_lang_code_guess,
                    "translation_error": "No translation needed"
                })

        results_for_sql = {
            "FileID": fileid,
            "DgDetectedLanguageCode": dg_detected_lang_code,
            "DgDetectedLanguageName": dg_detected_lang_name,
            "DgLanguageConfidence": dg_lang_confidence,
            "FullTranscriptTranslated": translated_full_transcript_info["translated_text"] if translated_full_transcript_info else None,
            "TranslationTargetLanguage": target_translation_lang,
            "TranslationError": translated_full_transcript_info.get("error") if translated_full_transcript_info else None,
            "TextSegmentLanguagesJson": json.dumps(text_segment_languages) if text_segment_languages else None,
            "TranslatedSpeakerSegmentsJson": json.dumps(translated_speaker_segments) if translated_speaker_segments else None,
            "Status": "Success"
        }

        # MODIFIED main SQL logging block
        if self.sql_helper:
            try:
                self.sql_helper.execute_sp("DG_LogLanguageDetection", (
                    results_for_sql["FileID"],
                    results_for_sql["DgDetectedLanguageCode"],
                    results_for_sql["DgDetectedLanguageName"],
                    results_for_sql["DgLanguageConfidence"],
                    results_for_sql["FullTranscriptTranslated"],
                    results_for_sql["TranslationTargetLanguage"],
                    results_for_sql["TranslationError"],
                    results_for_sql["TextSegmentLanguagesJson"],
                    results_for_sql["TranslatedSpeakerSegmentsJson"],
                    results_for_sql["Status"]
                ))
                print(f"Language Detection results for FileID {fileid} logged to SQL.")
            except Exception as e_sql:
                print(f"Error logging Language Detection results to SQL for FileID {fileid}: {e_sql}")
                results_for_sql["Status"] = "Error logging to SQL"
                # Log this SQL error itself using _log_sql_error
                self._log_sql_error(fileid, f"Primary SQL Log Failed: {e_sql}", results_for_sql.get("DgDetectedLanguageCode"), results_for_sql.get("TranslationTargetLanguage"), "Error logging to SQL")
        else:
            print("SQL Helper not configured. Skipping SQL logging for Language Detection.")

        print(f"Language Detection & Translation processing completed for FileID: {fileid}.")
        return {
            "status": results_for_sql["Status"],
            "fileid": fileid,
            "deepgram_language": {"code": dg_detected_lang_code, "name": dg_detected_lang_name, "confidence": dg_lang_confidence},
            "full_transcript_original": full_transcript,
            "full_transcript_translated": results_for_sql["FullTranscriptTranslated"],
            "segment_analysis": text_segment_languages,
            "speaker_translation_analysis": translated_speaker_segments
        }

# Example Usage (Conceptual) - Kept for reference, ensure it aligns if used
async def example_run():
    DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
    if not DEEPGRAM_API_KEY: print("DEEPGRAM_API_KEY not set."); return

    dummy_audio_path = "/home/ubuntu/dummy_lang_audio.wav"
    if not os.path.exists(dummy_audio_path):
        print(f"Creating dummy audio: {dummy_audio_path}")
        try:
            import wave
            with wave.open(dummy_audio_path, "w") as wf:
                wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(16000)
                wf.writeframes(b'\x00\x00' * 16000 * 5) # 5s silence
        except Exception as e: print(f"Failed to create dummy: {e}"); return

    class DummySQLHelper:
        def execute_sp(self, sp_name, params): print(f"[DummySQL] SP: {sp_name} with {params}")

    lang_detector = DgClassLanguageDetection(DEEPGRAM_API_KEY, sql_helper=DummySQLHelper())
    audio_info = {"file_path": dummy_audio_path}
    results = await lang_detector.main(audio_source_info=audio_info, fileid="LangTest001", target_translation_lang="es")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    # To run this example, ensure DEEPGRAM_API_KEY is set in your environment
    # and you have a suitable audio file (e.g., non-English or mixed language).
    # You might need to install missing libraries: pip install nltk langdetect googletrans==4.0.0-rc1 iso639-lang
    # asyncio.run(example_run())
    print("DgClassLanguageDetection example usage can be run by uncommenting the line above.")

