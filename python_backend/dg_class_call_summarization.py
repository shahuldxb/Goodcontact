"""
Deepgram Use Case 15: Call Summarization
Refactored into a Python class from deepgram_use_case_15_call_summarization.ipynb.

This class performs call summarization using Deepgram's native summarization
and other NLP techniques for keyword extraction, action items, and questions.
It requires audio access to perform transcription with summarization enabled.
"""

import os
import json
import asyncio
import re
import nltk
from textblob import TextBlob
from deepgram import Deepgram

# Attempt to import sumy and gensim, provide guidance if missing
try:
    from sumy.parsers.plaintext import PlaintextParser
    from sumy.nlp.tokenizers import Tokenizer as SumyTokenizer
    from sumy.summarizers.lsa import LsaSummarizer
    from sumy.summarizers.lex_rank import LexRankSummarizer
    from sumy.summarizers.luhn import LuhnSummarizer
    from sumy.nlp.stemmers import Stemmer
    from sumy.utils import get_stop_words as sumy_get_stop_words
    SUMY_AVAILABLE = True
except ImportError:
    print("sumy library not found. Extractive summarization with sumy will be unavailable. pip install sumy")
    SUMY_AVAILABLE = False

try:
    from gensim.summarization import keywords as gensim_keywords
    GENSIM_AVAILABLE = True
except ImportError:
    print("gensim library not found. Keyword extraction with gensim will be unavailable. pip install gensim")
    GENSIM_AVAILABLE = False

# Ensure NLTK resources are available
try:
    nltk.data.find("tokenizers/punkt.zip")
    nltk.data.find("corpora/stopwords.zip")
except LookupError:
    print("NLTK resources (punkt, stopwords) not found. Attempting to download...")
    try:
        nltk.download("punkt", quiet=True)
        nltk.download("stopwords", quiet=True)
        print("NLTK resources downloaded successfully.")
    except Exception as e:
        print(f"Failed to download NLTK resources: {e}. Please ensure they are available.")

class DgClassCallSummarization:
    def __init__(self, deepgram_api_key, sql_helper=None):
        self.deepgram_client = DeepgramClient(deepgram_api_key)
        self.sql_helper = sql_helper
        self._ensure_nltk_resources() # Call the NLTK resource check

    def _ensure_nltk_resources(self):
        resources = [("tokenizers/punkt", "punkt"), ("corpora/stopwords", "stopwords")]
        try:
            for path, name in resources:
                nltk.data.find(path)
        except LookupError:
            print(f"NLTK resource {name} not found again. Attempting to download...")
            try: nltk.download(name, quiet=True); print(f"NLTK resource {name} downloaded.")
            except Exception as e: print(f"Could not download NLTK resource {name}: {e}.")
        except Exception as e: print(f"An error occurred checking NLTK resources: {e}")

    async def _transcribe_audio_with_summarization(self, audio_source_info):
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
                utterances=True, paragraphs=True, detect_language=True,
                summarize="v2" # Enable Deepgram's summarization
            )
            print(f"Sending audio to Deepgram for transcription and summarization (Call Summarization class)...")
            # MODIFIED: Use listen.rest
            if "buffer" in source:
                response = await self.deepgram_client.listen.rest.v("1").transcribe_file(source, options)
            else:
                response = await self.deepgram_client.listen.rest.v("1").transcribe_url(source, options)
            return response.to_json(indent=None) # Return as JSON string
        except Exception as e:
            print(f"Error during Deepgram transcription with summarization: {e}")
            return None

    def _extract_details_from_dg_response(self, dg_response_json_str):
        full_transcript = ""
        deepgram_summary_text = ""
        detected_language = "Unknown"
        speaker_segments = []
        try:
            response = json.loads(dg_response_json_str)
            if not response or "results" not in response:
                return full_transcript, deepgram_summary_text, detected_language, speaker_segments
            
            results = response.get("results", {})
            channel = results.get("channels", [{}])[0]
            alternative = channel.get("alternatives", [{}])[0]
            
            full_transcript = alternative.get("transcript", "")
            detected_language = channel.get("detected_language", "Unknown")
            
            summary_obj = alternative.get("summaries", [{}])
            if summary_obj:
                deepgram_summary_text = summary_obj[0].get("summary", "")
            
            utterances = results.get("utterances")
            if utterances:
                for utt in utterances:
                    speaker_segments.append({
                        "speaker": utt.get("speaker", 0),
                        "text": utt.get("transcript", ""),
                        "start": utt.get("start"),
                        "end": utt.get("end"),
                        "duration": utt.get("duration")
                    })
            elif alternative.get("paragraphs", {}).get("paragraphs"):
                paragraphs_data = alternative.get("paragraphs", {})
                for para in paragraphs_data.get("paragraphs", []):
                    speaker = para.get("speaker", "Unknown")
                    text_parts = [sentence.get("text", "") for sentence in para.get("sentences", [])]
                    speaker_segments.append({"speaker": speaker, "text": " ".join(text_parts)})
            else:
                 speaker_segments.append({"speaker": "Unknown", "text": full_transcript})

        except Exception as e:
            print(f"Error extracting details from Deepgram response: {e}")
        return full_transcript, deepgram_summary_text, detected_language, speaker_segments

    def _analyze_speaker_data(self, speaker_segments):
        if not speaker_segments or not any(s.get("duration") is not None for s in speaker_segments):
            num_speakers = len(set(s["speaker"] for s in speaker_segments))
            speaker_word_count = {}
            for segment in speaker_segments:
                speaker_word_count[segment["speaker"]] = speaker_word_count.get(segment["speaker"], 0) + len(segment["text"].split())
            return {"num_speakers": num_speakers, "speaker_talk_time": {}, "speaker_word_count": speaker_word_count, "speaker_talk_ratio": {}}

        speakers = sorted(list(set(segment["speaker"] for segment in speaker_segments)))
        num_speakers = len(speakers)
        speaker_talk_time = {spk: 0.0 for spk in speakers}
        speaker_word_count = {spk: 0 for spk in speakers}

        for segment in speaker_segments:
            if segment.get("duration") is not None:
                speaker_talk_time[segment["speaker"]] += segment["duration"]
            speaker_word_count[segment["speaker"]] += len(segment["text"].split())
        
        total_talk_time = sum(speaker_talk_time.values())
        speaker_talk_ratio = {spk: (time / total_talk_time * 100) if total_talk_time > 0 else 0 for spk, time in speaker_talk_time.items()}
        
        return {
            "num_speakers": num_speakers,
            "speaker_talk_time": speaker_talk_time,
            "speaker_word_count": speaker_word_count,
            "speaker_talk_ratio": speaker_talk_ratio
        }

    def _extract_keywords_gensim_nltk(self, text, top_n=10, language="english"):
        if GENSIM_AVAILABLE:
            try:
                return gensim_keywords(text, words=top_n, split=True, lemmatize=True)
            except Exception as e:
                print(f"Gensim keyword extraction failed: {e}. Falling back to NLTK.")
        
        words = nltk.word_tokenize(text.lower())
        stop_words = set(nltk.corpus.stopwords.words(language))
        filtered_words = [word for word in words if word.isalnum() and word not in stop_words]
        fdist = nltk.FreqDist(filtered_words)
        return [word for word, _ in fdist.most_common(top_n)]

    def _generate_extractive_summary_sumy(self, text, method="lsa", sentences_count=3, language="english"):
        if not SUMY_AVAILABLE or not text:
            return "Sumy library not available or no text provided."
        try:
            parser = PlaintextParser.from_string(text, SumyTokenizer(language))
            stemmer = Stemmer(language)
            if method == "lsa": summarizer = LsaSummarizer(stemmer)
            elif method == "lexrank": summarizer = LexRankSummarizer(stemmer)
            elif method == "luhn": summarizer = LuhnSummarizer(stemmer)
            else: summarizer = LsaSummarizer(stemmer)
            summarizer.stop_words = sumy_get_stop_words(language)
            summary_sentences = summarizer(parser.document, sentences_count)
            return " ".join(str(s) for s in summary_sentences)
        except Exception as e:
            print(f"Error generating Sumy summary ({method}): {e}")
            return "Error in Sumy summarization."

    def _analyze_sentiment_textblob(self, text):
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        sentiment_label = "Neutral"
        if polarity > 0.1: sentiment_label = "Positive"
        elif polarity < -0.1: sentiment_label = "Negative"
        return {"polarity": polarity, "subjectivity": blob.sentiment.subjectivity, "label": sentiment_label}

    def _extract_action_items_regex(self, text):
        action_patterns = [
            r"(?i)(?:need to|should|have to|must|will|going to|plan to|follow up on|let me|I will|we will)\s+([^.!?]+(?:\s+(?:and|or)\s+[^.!?]+)*)",
            r"(?i)(?:action item[s]?|task[s]?):?\s*([^.!?]+)"
        ]
        action_items = []
        for pattern in action_patterns:
            matches = re.findall(pattern, text)
            for match_group in matches:
                action = match_group if isinstance(match_group, str) else match_group[0]
                action = action.strip()
                if action and len(action) > 5 and action not in action_items:
                    action_items.append(action)
        return action_items

    def _extract_questions_regex(self, text):
        sentences = nltk.sent_tokenize(text)
        questions = [s for s in sentences if s.strip().endswith("?")]
        indirect_patterns = [
            r"(?i)(?:I wonder|do you know|could you tell|I\\\'d like to know)\s+([^.!?]+[.!?])"
        ]
        for pattern in indirect_patterns:
            matches = re.findall(pattern, text)
            for match_group in matches:
                question = match_group if isinstance(match_group, str) else match_group[0]
                question = question.strip()
                if question and len(question) > 5 and question not in questions:
                    questions.append(question)
        return list(set(questions))

    # MODIFIED _log_sql_error method
    def _log_sql_error(self, fileid, error_message, dg_summary_at_error=None, language_at_error="Unknown", status="Error"):
        print(f"SQL logging error for FileID {fileid} (Call Summarization): {error_message}")
        if self.sql_helper:
            try:
                # Parameters for DG_LogCallSummarization:
                # FileID, DetectedLanguage, DeepgramSummary, ExtractiveLsaSummary, KeywordsJson, ActionItemsJson,
                # QuestionsJson, OverallSentimentLabel, OverallSentimentPolarity, SpeakerAnalysisJson, FullTranscriptLength, Status
                self.sql_helper.execute_sp("DG_LogCallSummarization", (
                    fileid,
                    language_at_error,
                    dg_summary_at_error, # DeepgramSummary
                    None,  # ExtractiveLsaSummary
                    None,  # KeywordsJson
                    None,  # ActionItemsJson
                    None,  # QuestionsJson
                    "Error", # OverallSentimentLabel
                    0.0,   # OverallSentimentPolarity
                    json.dumps({"error": str(error_message)[:200]}), # SpeakerAnalysisJson (using for error detail)
                    0,     # FullTranscriptLength
                    status # Status
                ))
            except Exception as e_sql_err:
                print(f"Further SQL error while logging initial Call Summarization error for FileID {fileid}: {e_sql_err}")

    async def main(self, audio_source_info, fileid):
        print(f"Starting Call Summarization for FileID: {fileid}")
        dg_response_json_str = await self._transcribe_audio_with_summarization(audio_source_info)

        # Initialize for error logging context
        current_dg_summary = None
        current_language = "Unknown"

        if not dg_response_json_str:
            # MODIFIED call to _log_sql_error
            if self.sql_helper: self._log_sql_error(fileid, "Transcription failed", current_dg_summary, current_language)
            return {"status": "Error", "fileid": fileid, "error": "Transcription failed"}

        full_transcript, dg_summary, lang, speaker_segments = self._extract_details_from_dg_response(dg_response_json_str)
        current_dg_summary = dg_summary # Update for logging context
        current_language = lang       # Update for logging context

        if not full_transcript:
            # MODIFIED call to _log_sql_error
            if self.sql_helper: self._log_sql_error(fileid, "No transcript extracted", current_dg_summary, current_language)
            return {"status": "Error", "fileid": fileid, "error": "No transcript extracted"}

        speaker_analysis_data = self._analyze_speaker_data(speaker_segments)
        keywords_list = self._extract_keywords_gensim_nltk(full_transcript, language=lang if lang != "Unknown" else "english")
        
        sumy_lsa_summary = self._generate_extractive_summary_sumy(full_transcript, method="lsa", language=lang if lang != "Unknown" else "english")
        
        overall_sentiment = self._analyze_sentiment_textblob(full_transcript)
        action_items_list = self._extract_action_items_regex(full_transcript)
        questions_list = self._extract_questions_regex(full_transcript)

        results_for_sql = {
            "FileID": fileid,
            "DetectedLanguage": lang,
            "DeepgramSummary": dg_summary,
            "ExtractiveLsaSummary": sumy_lsa_summary,
            "KeywordsJson": json.dumps(keywords_list) if keywords_list else None,
            "ActionItemsJson": json.dumps(action_items_list) if action_items_list else None,
            "QuestionsJson": json.dumps(questions_list) if questions_list else None,
            "OverallSentimentLabel": overall_sentiment.get("label"),
            "OverallSentimentPolarity": overall_sentiment.get("polarity"),
            "SpeakerAnalysisJson": json.dumps(speaker_analysis_data) if speaker_analysis_data else None,
            "FullTranscriptLength": len(full_transcript),
            "Status": "Success"
        }

        # MODIFIED main SQL logging block
        if self.sql_helper:
            try:
                self.sql_helper.execute_sp("DG_LogCallSummarization", (
                    results_for_sql["FileID"],
                    results_for_sql["DetectedLanguage"],
                    results_for_sql["DeepgramSummary"],
                    results_for_sql["ExtractiveLsaSummary"],
                    results_for_sql["KeywordsJson"],
                    results_for_sql["ActionItemsJson"],
                    results_for_sql["QuestionsJson"],
                    results_for_sql["OverallSentimentLabel"],
                    results_for_sql["OverallSentimentPolarity"],
                    results_for_sql["SpeakerAnalysisJson"],
                    results_for_sql["FullTranscriptLength"],
                    results_for_sql["Status"]
                ))
                print(f"Call Summarization results for FileID {fileid} logged to SQL.")
            except Exception as e_sql:
                print(f"Error logging Call Summarization results to SQL for FileID {fileid}: {e_sql}")
                results_for_sql["Status"] = "Error logging to SQL"
                # Log this SQL error itself using _log_sql_error
                self._log_sql_error(fileid, f"Primary SQL Log Failed: {e_sql}", results_for_sql.get("DeepgramSummary"), results_for_sql.get("DetectedLanguage"), "Error logging to SQL")
        else:
            print("SQL Helper not configured. Skipping SQL logging for Call Summarization.")

        print(f"Call Summarization processing completed for FileID: {fileid}.")
        return {
            "status": results_for_sql["Status"],
            "fileid": fileid,
            "deepgram_summary": dg_summary,
            "lsa_summary": sumy_lsa_summary,
            "keywords": keywords_list,
            "action_items": action_items_list,
            "questions": questions_list,
            "overall_sentiment": overall_sentiment,
            "speaker_analysis": speaker_analysis_data,
            "full_transcript_snippet": full_transcript[:200] + "..."
        }

# Example Usage (Conceptual)
async def example_run():
    DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
    if not DEEPGRAM_API_KEY: print("DEEPGRAM_API_KEY not set."); return

    dummy_audio_path = "/home/ubuntu/dummy_summarization_audio.wav"
    if not os.path.exists(dummy_audio_path):
        print(f"Creating dummy audio: {dummy_audio_path}")
        try:
            import wave
            with wave.open(dummy_audio_path, "w") as wf:
                wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(16000)
                wf.writeframes(b'\x00\x00' * 16000 * 10) # 10s silence
        except Exception as e: print(f"Failed to create dummy: {e}"); return

    class DummySQLHelper:
        def execute_sp(self, sp_name, params): print(f"[DummySQL] SP: {sp_name} with {params}")

    summarizer_analyzer = DgClassCallSummarization(DEEPGRAM_API_KEY, sql_helper=DummySQLHelper())
    audio_info = {"file_path": dummy_audio_path}
    results = await summarizer_analyzer.main(audio_source_info=audio_info, fileid="SUMTest001")
    print("\n--- Example Call Summarization Results ---")
    if results: print(json.dumps(results, indent=2))

if __name__ == "__main__":
    # asyncio.run(example_run())
    print("DgClassCallSummarization defined.")

