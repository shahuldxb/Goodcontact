"""
Deepgram Use Case 1: Sentiment Analysis to Assess Emotional Tone of Customers
Refactored into a Python class.
"""

import os
import re
import json
import asyncio
import nltk
import pandas as pd
from deepgram import DeepgramClient, PrerecordedOptions, FileSource # Corrected import for Deepgram v3+
import datetime # Ensure datetime is imported

# Import sentiment analysis tools
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob

# Ensure NLTK resources are available
try:
    nltk.data.find("sentiment/vader_lexicon.zip")
except LookupError: # Corrected exception type
    print("NLTK: vader_lexicon not found. Downloading...")
    nltk.download("vader_lexicon")
try:
    nltk.data.find("tokenizers/punkt")
except LookupError: # Corrected exception type
    print("NLTK: punkt not found. Downloading...")
    nltk.download("punkt")

class DgClassSentimentAnalysis:
    def __init__(self, deepgram_api_key, sql_helper=None):
        """
        Initializes the sentiment analysis class.
        Args:
            deepgram_api_key (str): The Deepgram API key.
            sql_helper (SQLServerConnector): An instance of the SQLServerConnector class for database interactions.
        """
        self.deepgram_client = DeepgramClient(deepgram_api_key) # Use DeepgramClient
        self.sql_helper = sql_helper
        self.sid = SentimentIntensityAnalyzer()
        self.class_name = self.__class__.__name__

    def dg_func_simple_sent_tokenize(self, text):
        """A simple sentence tokenizer that splits text on common sentence-ending punctuation."""
        text = re.sub(r"([.!?])\s+", r"\1<SPLIT>", text)
        if not text.endswith((".", "!", "?", "<SPLIT>")):
            text += "<SPLIT>"
        sentences = text.split("<SPLIT>")
        return [s.strip() for s in sentences if s.strip()]

    def dg_func_analyze_sentiment_nltk(self, text):
        """Analyze sentiment using NLTK VADER."""
        return self.sid.polarity_scores(text)

    def dg_func_analyze_sentiment_textblob(self, text):
        """Analyze sentiment using TextBlob."""
        blob = TextBlob(text)
        return {"polarity": blob.sentiment.polarity, "subjectivity": blob.sentiment.subjectivity}

    def dg_func_get_sentiment_label(self, compound_score):
        """Convert compound score to sentiment label."""
        if compound_score >= 0.05:
            return "Positive"
        elif compound_score <= -0.05:
            return "Negative"
        else:
            return "Neutral"

    def dg_func_extract_transcript_from_dg_response(self, response_json):
        """
        Extract transcript from Deepgram JSON response.
        Returns:
            tuple: (formatted_transcript, raw_transcript, detected_language)
        """
        if not response_json or "results" not in response_json:
            return "", "", "Unknown"

        results = response_json.get("results", {})
        channels = results.get("channels", [{}])
        if not channels:
            return "", "", "Unknown"
        
        channel = channels[0]
        detected_language = channel.get("detected_language", "Unknown")
        alternatives = channel.get("alternatives", [{}])
        if not alternatives:
            return "", "", detected_language
        
        alternative = alternatives[0]
        raw_transcript = alternative.get("transcript", "")
        paragraphs_data = alternative.get("paragraphs", {})

        formatted_transcript = raw_transcript
        if paragraphs_data and "paragraphs" in paragraphs_data:
            formatted_text_parts = []
            for para in paragraphs_data.get("paragraphs", []):
                speaker = para.get("speaker", "Unknown") # Default to Unknown if not present
                sentences_data = para.get("sentences", [])
                text = " ".join([sentence.get("text", "") for sentence in sentences_data])
                formatted_text_parts.append(f"Speaker {speaker}: {text}")
            formatted_transcript = "\n\n".join(formatted_text_parts)
        
        return formatted_transcript, raw_transcript, detected_language

    def dg_func_analyze_text_by_sentence(self, text):
        """
        Analyze text sentiment at the sentence level.
        Returns:
            list: A list of dictionaries, each containing sentiment scores for a sentence.
        """
        sentences = self.dg_func_simple_sent_tokenize(text)
        sentence_sentiments = []
        for sentence in sentences:
            if len(sentence.strip()) > 0:
                nltk_sentiment = self.dg_func_analyze_sentiment_nltk(sentence)
                textblob_sentiment = self.dg_func_analyze_sentiment_textblob(sentence)
                sentiment_label = self.dg_func_get_sentiment_label(nltk_sentiment["compound"])
                
                sentence_sentiments.append({
                    "sentence": sentence,
                    "nltk_compound": nltk_sentiment["compound"],
                    "nltk_positive": nltk_sentiment["pos"],
                    "nltk_neutral": nltk_sentiment["neu"],
                    "nltk_negative": nltk_sentiment["neg"],
                    "textblob_polarity": textblob_sentiment["polarity"],
                    "textblob_subjectivity": textblob_sentiment["subjectivity"],
                    "sentiment_label": sentiment_label
                })
        return sentence_sentiments

    async def main(self, deepgram_response_json, fileid):
        """
        Main function to process pre-transcribed Deepgram JSON for sentiment analysis.
        Args:
            deepgram_response_json (dict): The pre-fetched Deepgram API JSON response.
            fileid (str): The file ID (e.g., from deepgram_assets table, or a unique identifier like blob name).
        Returns:
            dict: A dictionary containing the sentiment analysis results.
        """
        func_name = "main"
        start_time = datetime.datetime.now(datetime.timezone.utc)
        print(f"[{self.class_name}] Starting sentiment analysis for fileid: {fileid}")
        
        detected_language = "Unknown" # Default

        if not deepgram_response_json:
            print(f"[{self.class_name}] No Deepgram response provided for fileid: {fileid}. Cannot proceed.")
            if self.sql_helper:
                # SP: @FileID, @DetectedLanguage, @OverallSentimentLabel, @OverallNltkCompoundScore, @SentenceSentimentsJson, @Status
                self.sql_helper.execute_sp(
                    sp_name="DG_LogSentimentAnalysis", 
                    params=(fileid, detected_language, "Error: No Deepgram response", None, None, "Error")
                )
            end_time = datetime.datetime.now(datetime.timezone.utc)
            if self.sql_helper:
                self.sql_helper.execute_sp("DG_LogTimeElapsed", (self.class_name, func_name, str(fileid), start_time, end_time, (end_time - start_time).total_seconds()))
            return {"error": "No Deepgram response provided", "fileid": fileid, "status": "Error"}

        # Step 1: Extract transcript from the provided JSON response
        formatted_transcript, raw_transcript, detected_language = self.dg_func_extract_transcript_from_dg_response(deepgram_response_json)
        
        if not raw_transcript:
            print(f"[{self.class_name}] No transcript extracted from Deepgram response for fileid: {fileid}. Cannot proceed.")
            if self.sql_helper:
                # SP: @FileID, @DetectedLanguage, @OverallSentimentLabel, @OverallNltkCompoundScore, @SentenceSentimentsJson, @Status
                self.sql_helper.execute_sp(
                    sp_name="DG_LogSentimentAnalysis", 
                    params=(fileid, detected_language, "Error: No transcript extracted", None, None, "Error")
                )
            end_time = datetime.datetime.now(datetime.timezone.utc)
            if self.sql_helper:
                self.sql_helper.execute_sp("DG_LogTimeElapsed", (self.class_name, func_name, str(fileid), start_time, end_time, (end_time - start_time).total_seconds()))
            return {"error": "No transcript extracted", "fileid": fileid, "status": "Error"}

        print(f"[{self.class_name}] Transcript extracted for fileid: {fileid}. Language: {detected_language}. Analyzing sentiment...")

        # Step 2: Analyze sentiment
        sentence_sentiments = self.dg_func_analyze_text_by_sentence(raw_transcript)
        if not sentence_sentiments:
            print(f"[{self.class_name}] Sentiment analysis yielded no results for fileid: {fileid}.")
            if self.sql_helper:
                 # SP: @FileID, @DetectedLanguage, @OverallSentimentLabel, @OverallNltkCompoundScore, @SentenceSentimentsJson, @Status
                 self.sql_helper.execute_sp(
                    sp_name="DG_LogSentimentAnalysis", 
                    params=(fileid, detected_language, "NoSentimentResults", None, None, "Error")
                )
            end_time = datetime.datetime.now(datetime.timezone.utc)
            if self.sql_helper:
                self.sql_helper.execute_sp("DG_LogTimeElapsed", (self.class_name, func_name, str(fileid), start_time, end_time, (end_time - start_time).total_seconds()))
            return {"error": "Sentiment analysis yielded no results", "fileid": fileid, "status": "Error"}

        df = pd.DataFrame(sentence_sentiments)
        overall_nltk_compound = df["nltk_compound"].mean() if not df.empty else 0
        overall_sentiment_label = self.dg_func_get_sentiment_label(overall_nltk_compound)
        sentence_sentiments_json_str = json.dumps(sentence_sentiments)
        
        # For results_payload, include other overall scores as before
        overall_textblob_polarity = df["textblob_polarity"].mean() if not df.empty else 0
        overall_textblob_subjectivity = df["textblob_subjectivity"].mean() if not df.empty else 0
        overall_nltk_positive = df["nltk_positive"].mean() if not df.empty else 0
        overall_nltk_neutral = df["nltk_neutral"].mean() if not df.empty else 0
        overall_nltk_negative = df["nltk_negative"].mean() if not df.empty else 0
        
        results_payload = {
            "fileid": fileid,
            "detected_language": detected_language,
            "raw_transcript": raw_transcript,
            "formatted_transcript": formatted_transcript,
            "overall_sentiment_label": overall_sentiment_label,
            "overall_nltk_compound_score": float(overall_nltk_compound),
            "overall_nltk_positive": float(overall_nltk_positive),
            "overall_nltk_neutral": float(overall_nltk_neutral),
            "overall_nltk_negative": float(overall_nltk_negative),
            "overall_textblob_polarity": float(overall_textblob_polarity),
            "overall_textblob_subjectivity": float(overall_textblob_subjectivity),
            "sentence_sentiments_json": sentence_sentiments_json_str,
            "status": "Success"
        }
        print(f"[{self.class_name}] Sentiment analysis completed for fileid: {fileid}. Results ready.")

        # Step 3: Log results to SQL
        status_to_log = results_payload.get("status", "Unknown")
        if self.sql_helper:
            # SP: @FileID, @DetectedLanguage, @OverallSentimentLabel, @OverallNltkCompoundScore, @SentenceSentimentsJson, @Status
            self.sql_helper.execute_sp(
                sp_name="DG_LogSentimentAnalysis", 
                params=(
                    fileid,
                    detected_language,
                    overall_sentiment_label,
                    float(overall_nltk_compound), # This is fine, defaults to 0 if df empty
                    sentence_sentiments_json_str,
                    status_to_log
                )
            )

        end_time = datetime.datetime.now(datetime.timezone.utc)
        if self.sql_helper:
            self.sql_helper.execute_sp("DG_LogTimeElapsed", (self.class_name, func_name, str(fileid), start_time, end_time, (end_time - start_time).total_seconds()))
        return results_payload

# Example usage (for testing purposes)
async def example_run():
    DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY", "YOUR_DG_API_KEY_HERE") # User should set this
    if DEEPGRAM_API_KEY == "YOUR_DG_API_KEY_HERE":
        print("Please set your DEEPGRAM_API_KEY environment variable or update the script.")
        # return # Allow to run with dummy key for this test as we don't call DG API

    # Mock SQLHelper for standalone testing if sql_helper.py is not used
    class MockSQLHelper:
        def connect(self):
            print("[Internal MockSQLHelper] connect() called")
            return True, True
        def execute_sp(self, sp_name, params):
            # Convert datetime objects in params to ISO format string for printing
            formatted_params = []
            for p in params:
                if isinstance(p, datetime.datetime):
                    formatted_params.append(p.isoformat())
                else:
                    formatted_params.append(p)
            print(f"[Internal MockSQLHelper] execute_sp called: {sp_name} with params: {tuple(formatted_params)}")
        def close(self):
            print("[Internal MockSQLHelper] close() called")

    mock_sql_helper = MockSQLHelper()
    sentiment_analyzer = DgClassSentimentAnalysis(deepgram_api_key=DEEPGRAM_API_KEY, sql_helper=mock_sql_helper)
    
    example_fileid = "test_audio_001"
    
    dummy_dg_response = {
        "metadata": {"request_id": "dummy"},
        "results": {
            "channels": [
                {
                    "detected_language": "en",
                    "alternatives": [
                        {
                            "transcript": "I had a really great experience today. The service was excellent and I am very happy. However, the wait time was a bit long.",
                            "paragraphs": {
                                "transcript": "I had a really great experience today. The service was excellent and I am very happy. However, the wait time was a bit long.",
                                "paragraphs": [
                                    {
                                        "speaker": 0,
                                        "sentences": [
                                            {"text": "I had a really great experience today."},
                                            {"text": "The service was excellent and I am very happy."},
                                            {"text": "However, the wait time was a bit long."}
                                        ],
                                        "start": 0.0, "end": 10.0, "num_words": 20 # Added dummy values
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
    }

    results = await sentiment_analyzer.main(deepgram_response_json=dummy_dg_response, fileid=example_fileid)
    if results and results.get("status") == "Success":
        print("\n--- Example Run Results ---")
        print(f"Overall Sentiment: {results.get('overall_sentiment_label')}")
        print(f"Overall NLTK Compound Score: {results.get('overall_nltk_compound_score')}")
    else:
        print("\n--- Example Run Failed ---")
        print(results)

if __name__ == "__main__":
    # This ensures that nltk downloads happen in a blocking way if needed, before async code
    # The class instantiation already triggers downloads if necessary.
    asyncio.run(example_run())

