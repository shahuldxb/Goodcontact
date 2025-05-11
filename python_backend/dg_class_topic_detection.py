"""
Deepgram Use Case 9: Topic Detection and Summarization
Refactored into a Python class.

Note: This class uses NLTK for text preprocessing and scikit-learn for LDA topic modeling.
These libraries would need to be installed in the execution environment.
Deepgram's own summarization (if enabled and returned) is also captured.
"""

import os
import json
import asyncio
import re
import requests

# Attempt to import NLTK and scikit-learn, provide guidance if missing
try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize, sent_tokenize
    from nltk.stem import WordNetLemmatizer
    nltk.data.find("corpora/wordnet.zip")
    nltk.data.find("tokenizers/punkt.zip")
    nltk.data.find("corpora/stopwords.zip")
except LookupError:
    print("NLTK resources (wordnet, punkt, stopwords) not found. Attempting to download...")
    try:
        nltk.download("wordnet", quiet=True)
        nltk.download("punkt", quiet=True)
        nltk.download("stopwords", quiet=True)
        print("NLTK resources downloaded successfully.")
    except Exception as e:
        print(f"Failed to download NLTK resources: {e}. Please ensure they are available.")
except ImportError:
    print("NLTK library not found. Please install it: pip install nltk")

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.decomposition import LatentDirichletAllocation
except ImportError:
    print("scikit-learn library not found. Please install it: pip install scikit-learn")

class DgClassTopicDetection:
    def __init__(self, deepgram_api_key, sql_helper=None):
        """
        Initializes the Topic Detection class.
        Args:
            deepgram_api_key (str): The Deepgram API key.
            sql_helper (SQLHelper): An instance of the SQLHelper class for database interactions.
        """
        self.deepgram_api_key = deepgram_api_key # Store API key for direct API calls
        self.sql_helper = sql_helper
        self._ensure_nltk_resources()

    def _ensure_nltk_resources(self):
        """Downloads NLTK resources if not already present."""
        resources = [("corpora/wordnet.zip", "wordnet"), ("tokenizers/punkt.zip", "punkt"), ("corpora/stopwords.zip", "stopwords")]
        try:
            for path, name in resources:
                nltk.data.find(path) # Corrected to use the .zip path for find
        except LookupError as e_lookup:
            # Check if it's the specific error that means it's not found
            # The actual resource name for download might be different from the .zip path component
            resource_name_for_download = name 
            print(f"NLTK resource {resource_name_for_download} not found. Downloading...")
            try:
                nltk.download(resource_name_for_download, quiet=True)
                print(f"NLTK resource {resource_name_for_download} downloaded.")
            except Exception as e_download:
                print(f"Could not download NLTK resource {resource_name_for_download}: {e_download}. Manual download may be required.")
        except Exception as e_general:
            print(f"An error occurred checking NLTK resources: {e_general}")

    async def dg_func_transcribe_audio(self, audio_file_path, enable_dg_summarize=True):
        """
        Transcribe audio using Deepgram API, optionally enabling Deepgram's summarization.
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
                    "smart_format": "true"
                }
                if enable_dg_summarize:
                    params["summarize"] = "v2"
                
                # Set up headers with API key
                headers = {
                    "Authorization": f"Token {self.deepgram_api_key}",
                    "Content-Type": f"audio/{file_type}"
                }
                
                print(f"Sending audio file {audio_file_path} to Deepgram for transcription (Topic Detection)...")
                
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
            print(f"Error during transcription (Topic Detection) for {audio_file_path}: {e}")
            return None

    def dg_func_extract_transcript_and_dg_summary(self, response):
        """
        Extracts transcript and Deepgram summary (if available).
        """
        if not response or "results" not in response:
            return "", "Unknown", ""
        
        results = response.get("results", {})
        channels = results.get("channels", [{}])
        if not channels: return "", "Unknown", ""
        
        channel = channels[0]
        detected_language = channel.get("detected_language", "Unknown")
        alternatives = channel.get("alternatives", [{}])
        if not alternatives: return "", detected_language, ""
        
        alternative = alternatives[0]
        raw_transcript = alternative.get("transcript", "")
        
        dg_summary_text = ""
        summary_obj = response.get("summary") # For summarize:v2, summary is top-level
        if summary_obj and isinstance(summary_obj, dict):
            dg_summary_text = summary_obj.get("short") # Or .long, .gist etc.
        elif not dg_summary_text: # Fallback to older summary path if new one isn't there
            summary_obj_alt = alternative.get("summary")
            if summary_obj_alt and isinstance(summary_obj_alt, dict):
                dg_summary_text = summary_obj_alt.get("text", "")
            elif isinstance(summary_obj_alt, str):
                 dg_summary_text = summary_obj_alt

        return raw_transcript, detected_language, dg_summary_text

    def _preprocess_text_for_lda(self, text, language='english'):
        """Preprocesses text for LDA: tokenization, stopword removal, lemmatization."""
        try:
            tokens = word_tokenize(text.lower())
            supported_stopwords_langs = stopwords.fileids()
            if language not in supported_stopwords_langs:
                print(f"Warning: Language '{language}' not supported for NLTK stopwords. Defaulting to English.")
                language = 'english'
            stop_words = set(stopwords.words(language))
            lemmatizer = WordNetLemmatizer()
            processed_tokens = [
                lemmatizer.lemmatize(token) for token in tokens 
                if token.isalpha() and token not in stop_words and len(token) > 2
            ]
            return " ".join(processed_tokens)
        except Exception as e:
            print(f"Error in _preprocess_text_for_lda: {e}")
            return ""

    def dg_func_detect_topics_with_lda(self, transcript, num_topics=5, num_words_per_topic=7, language='english'):
        """
        Detects topics in a transcript using Latent Dirichlet Allocation (LDA).
        """
        if not transcript:
            return [], [] 
        
        try:
            sentences = sent_tokenize(transcript)
            if not sentences:
                print("No sentences found in transcript for LDA.")
                return [], []
                
            if len(sentences) < num_topics:
                print(f"Warning: Number of sentences ({len(sentences)}) is less than num_topics ({num_topics}). Adjusting num_topics.")
                num_topics = max(1, len(sentences))

            preprocessed_sentences = [self._preprocess_text_for_lda(s, language) for s in sentences]
            preprocessed_sentences = [s for s in preprocessed_sentences if s.strip()] 

            if not preprocessed_sentences or len(preprocessed_sentences) < num_topics:
                print("Not enough valid preprocessed sentences for LDA topic modeling after cleaning.")
                return [], []
            
            # Ensure min_df is not greater than the number of documents (preprocessed_sentences)
            actual_min_df = 2
            if len(preprocessed_sentences) < actual_min_df:
                actual_min_df = 1 # Adjust min_df if fewer than 2 documents

            vectorizer = TfidfVectorizer(max_df=0.95, min_df=actual_min_df, stop_words=language if language == "english" else None, max_features=1000)
            tfidf_matrix = vectorizer.fit_transform(preprocessed_sentences)
            
            if tfidf_matrix.shape[1] == 0: # No features extracted
                print("No features extracted by TF-IDF vectorizer. Cannot perform LDA.")
                return [], []

            if num_topics > tfidf_matrix.shape[1]:
                print(f"Warning: num_topics ({num_topics}) > num_features ({tfidf_matrix.shape[1]}). Adjusting num_topics.")
                num_topics = max(1, tfidf_matrix.shape[1])

            lda_model = LatentDirichletAllocation(n_components=num_topics, random_state=42, learning_method='online')
            lda_topic_matrix = lda_model.fit_transform(tfidf_matrix)

            feature_names = vectorizer.get_feature_names_out()
            extracted_topics = []
            for topic_idx, topic_weights in enumerate(lda_model.components_):
                top_word_indices = topic_weights.argsort()[:-num_words_per_topic-1:-1]
                top_words = [feature_names[i] for i in top_word_indices]
                extracted_topics.append({"topic_id": topic_idx, "keywords": top_words, "score": float(topic_weights.sum())})
            
            sentence_topic_assignments = []
            # Map preprocessed sentences back to original sentences for assignment
            # This assumes that preprocessed_sentences directly correspond to the rows in lda_topic_matrix
            # and that their order is maintained relative to the original sentences that yielded non-empty preprocessed text.
            original_sentence_index = 0
            for i, preproc_sentence_text in enumerate(preprocessed_sentences):
                # Find the corresponding original sentence (this is a simplification)
                # A more robust mapping would be needed if original sentences could be empty or entirely removed by preprocessing.
                while original_sentence_index < len(sentences) and not self._preprocess_text_for_lda(sentences[original_sentence_index], language).strip():
                    original_sentence_index += 1
                
                if original_sentence_index < len(sentences):
                    dominant_topic_idx = lda_topic_matrix[i].argmax()
                    topic_confidence = lda_topic_matrix[i][dominant_topic_idx]
                    sentence_topic_assignments.append({
                        "sentence": sentences[original_sentence_index],
                        "assigned_topic_id": int(dominant_topic_idx),
                        "confidence": float(topic_confidence)
                    })
                    original_sentence_index += 1
                else:
                    # This case should ideally not be hit if logic is correct
                    print(f"Warning: Could not map preprocessed sentence {i} back to an original sentence.")

            return sorted(extracted_topics, key=lambda x: x["score"], reverse=True), sentence_topic_assignments
        except Exception as e:
            print(f"Error in dg_func_detect_topics_with_lda: {e}")
            return [], []

    async def main(self, dg_response_json_str, fileid, local_audio_path=None, num_lda_topics=5, num_words_per_topic=7, enable_dg_summarize=True, **kwargs):
        """
        Main function for topic detection and summarization.
        """
        print(f"Starting Topic Detection for fileid: {fileid}")

        results_payload = {
            "fileid": fileid,
            "audio_file_path": local_audio_path,
            "detected_language": "Unknown",
            "raw_transcript_length": 0,
            "raw_transcript_snippet_for_log": None,
            "deepgram_summary": None,
            "lda_detected_topics": [],
            "lda_sentence_topic_assignments": [],
            "status": "Error",
            "error": None
        }
        raw_transcript = ""

        try:
            transcription_response = None
            if dg_response_json_str:
                print(f"Using provided transcription for Topic Detection, fileid: {fileid}")
                transcription_response = json.loads(dg_response_json_str)
            elif local_audio_path:
                print(f"Transcribing {local_audio_path} for Topic Detection, fileid: {fileid}")
                transcription_response = await self.dg_func_transcribe_audio(local_audio_path, enable_dg_summarize)
            else:
                raise ValueError("No Deepgram response string or local audio path provided for Topic Detection.")

            if not transcription_response:
                results_payload["error"] = "Transcription failed or produced no response."
                print(results_payload["error"])
                return results_payload

            raw_transcript, detected_language, dg_summary = self.dg_func_extract_transcript_and_dg_summary(transcription_response)
            results_payload["detected_language"] = detected_language
            results_payload["raw_transcript_length"] = len(raw_transcript)
            results_payload["raw_transcript_snippet_for_log"] = raw_transcript[:1000] if raw_transcript else None
            results_payload["deepgram_summary"] = dg_summary

            if not raw_transcript:
                results_payload["error"] = "No transcript extracted from Deepgram response."
                print(results_payload["error"])
                return results_payload

            lda_topics, sentence_assignments = self.dg_func_detect_topics_with_lda(
                raw_transcript, 
                num_lda_topics, 
                num_words_per_topic, 
                detected_language.lower() if detected_language != "Unknown" else "english"
            )
            results_payload["lda_detected_topics"] = lda_topics
            results_payload["lda_sentence_topic_assignments"] = sentence_assignments
            results_payload["status"] = "Success"

        except Exception as e_main:
            error_msg = f"Error in DgClassTopicDetection main for fileid {fileid}: {e_main}"
            print(error_msg)
            results_payload["status"] = "Error"
            results_payload["error"] = error_msg
        
        # SQL Logging Block
        if self.sql_helper:
            try:
                fileid_val = results_payload.get("fileid")
                dg_summary_val = results_payload.get("deepgram_summary")
                lda_topics_json_val = json.dumps(results_payload.get("lda_detected_topics")) if results_payload.get("lda_detected_topics") else None
                # sentence_assignments_json_val = json.dumps(results_payload.get("lda_sentence_topic_assignments")) # This can be very large
                transcript_snippet_val = results_payload.get("raw_transcript_snippet_for_log")
                status_for_log = results_payload.get("status", "Error")
                error_for_log = results_payload.get("error")

                params = (
                    fileid_val,
                    dg_summary_val,
                    lda_topics_json_val,
                    # sentence_assignments_json_val, # Consider if this is too large for a single SP param
                    transcript_snippet_val,
                    status_for_log,
                    error_for_log
                )
                self.sql_helper.execute_sp("DG_LogTopicDetection", params)
                print(f"Topic Detection results for fileid: {fileid_val} logged to SQL with status: {status_for_log}.")
            except Exception as e_sql:
                sql_error_msg = f"Error logging Topic Detection results to SQL for fileid {fileid_val}: {e_sql}"
                print(sql_error_msg)
                results_payload["status_sql_logging"] = "Error_SQLLogging"
                results_payload["error_sql_logging"] = sql_error_msg
        else:
            print(f"SQL Helper not available. Skipping SQL logging for fileid: {fileid}.")

        print(f"Topic Detection processing completed for fileid: {fileid}. Final status: {results_payload.get('status')}")
        return results_payload

# Example usage
async def example_run():
    DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "YOUR_DG_API_KEY_PLACEHOLDER")
    if DEEPGRAM_API_KEY == "YOUR_DG_API_KEY_PLACEHOLDER":
        print("Warning: DEEPGRAM_API_KEY not set. Live API calls will likely fail.")

    class DummySQLHelper:
        def execute_sp(self, sp_name, params):
            print(f"[DummySQLHelper] Called {sp_name} with params: {params}")

    dummy_sql_helper = DummySQLHelper()
    topic_detector = DgClassTopicDetection(deepgram_api_key=DEEPGRAM_API_KEY, sql_helper=dummy_sql_helper)
    example_fileid = "topic_test_001"

    dummy_audio_path = "/home/ubuntu/dummy_topic_audio.wav"
    if not os.path.exists(dummy_audio_path):
        try:
            import wave
            with wave.open(dummy_audio_path, "w") as wf:
                wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(16000)
                frames = b"\x00\x00" * 16000 * 30 
                wf.writeframes(frames)
            print(f"Created dummy audio file: {dummy_audio_path}")
        except Exception as e_wave:
            print(f"Could not create dummy audio file: {e_wave}. Topic test with live call might fail.")

    # Test 1: Using a dummy dg_response_json_str
    print("\n--- Test 1: Using dummy_dg_response_json_str ---")
    dummy_response_str = json.dumps({
        "results": {
            "channels": [{
                "detected_language": "en",
                "alternatives": [{
                    "transcript": "The quick brown fox jumps over the lazy dog. The lazy dog slept in the sun. The sun is very hot today. Foxes are cunning animals."
                }]
            }]
        },
        "summary": {"short": "A fox jumped over a dog. The dog was lazy. The sun was hot."}
    })
    results1 = await topic_detector.main(dg_response_json_str=dummy_response_str, fileid=example_fileid, local_audio_path=None)
    if results1:
        print(f"Status: {results1.get('status')}")
        if results1.get('error'): print(f"Error: {results1.get('error')}")
        print(f"Deepgram Summary: {results1.get('deepgram_summary')}")
        print("LDA Topics:", json.dumps(results1.get("lda_detected_topics"), indent=2))

    # Test 2: Using local_audio_path (will make a live call if API key is valid)
    # print("\n--- Test 2: Using local_audio_path (Live API Call) ---")
    # results2 = await topic_detector.main(dg_response_json_str=None, fileid="topic_test_002", local_audio_path=dummy_audio_path)
    # if results2:
    #     print(f"Status: {results2.get("status")}")
    #     if results2.get("error"): print(f"Error: {results2.get("error")}")
    #     print(f"Deepgram Summary: {results2.get("deepgram_summary")}")
    #     print("LDA Topics:", json.dumps(results2.get("lda_detected_topics"), indent=2))

if __name__ == "__main__":
    # import nest_asyncio
    # nest_asyncio.apply()
    asyncio.run(example_run())


