"""
Deepgram Use Case 6: Detection of Forbidden Phrases
Refactored into a Python class.
"""

import os
import re
import json
import asyncio
from deepgram import Deepgram # Assuming DeepgramClient is imported as Deepgram

# Define default forbidden phrases if none are provided
DEFAULT_FORBIDDEN_PHRASES = {
    'financial_promises': [
        "guaranteed returns", "guaranteed profit", "can't lose", "risk-free investment",
        "double your money", "triple your money", "100% return", "get rich quick"
    ],
    'misleading_claims': [
        "scientifically proven", "clinically proven", "doctors recommend", "studies show",
        "miracle cure", "secret formula", "revolutionary breakthrough"
    ],
    'unauthorized_disclosures': [
        "between you and me", "off the record", "don't tell anyone", "keep this confidential",
        "this is just for you", "not supposed to tell you"
    ],
    'discriminatory_language': [
        "those people", "you people", "your kind", "these types",
        "not like the others", "not like them"
    ],
    'unauthorized_offers': [
        "special deal just for you", "unofficial discount", "under the table",
        "between us only", "management doesn't know"
    ]
}

class DgClassForbiddenPhrases:
    def __init__(self, deepgram_api_key, sql_helper=None):
        """
        Initializes the Forbidden Phrases Detection class.
        Args:
            deepgram_api_key (str): The Deepgram API key.
            sql_helper (SQLHelper): An instance of the SQLHelper class for database interactions.
        """
        self.deepgram = Deepgram(deepgram_api_key)
        self.sql_helper = sql_helper

    async def dg_func_transcribe_audio_for_phrases(self, audio_file_path, phrases_to_detect):
        """
        Transcribe audio using Deepgram API with specific phrases for detection.
        Args:
            audio_file_path (str): Path to the audio file.
            phrases_to_detect (list): List of phrases for Deepgram to spot.
        Returns:
            dict: The Deepgram API response.
        """
        try:
            with open(audio_file_path, "rb") as audio:
                source = {"buffer": audio, "mimetype": "audio/wav"}
                options = {
                    "punctuate": True, "diarize": True, "detect_language": True,
                    "model": "nova-2", "smart_format": True,
                    "keywords": phrases_to_detect # Using keywords feature for phrase spotting
                }
                print(f"Sending audio file {audio_file_path} to Deepgram for phrase detection (Forbidden Phrases)...")
                response = await self.deepgram.transcription.prerecorded(source, options)
                return response
        except Exception as e:
            print(f"Error during transcription for phrase detection (Forbidden Phrases) for {audio_file_path}: {e}")
            return None

    def dg_func_extract_transcript_and_language(self, response):
        """
        Extracts raw transcript and detected language from Deepgram response.
        """
        if not response or "results" not in response:
            return "", "Unknown"
        results = response.get("results", {})
        channels = results.get("channels", [{}])
        if not channels: return "", "Unknown"
        channel = channels[0]
        detected_language = channel.get("detected_language", "Unknown")
        alternatives = channel.get("alternatives", [{}])
        if not alternatives: return "", detected_language
        raw_transcript = alternatives[0].get("transcript", "")
        return raw_transcript, detected_language

    def dg_func_extract_detected_phrases_from_dg(self, response, forbidden_phrases_map):
        """
        Extracts detected forbidden phrases from Deepgram response based on the `keywords` (search) feature.
        Categorizes them based on the provided forbidden_phrases_map.
        """
        detected_phrases_categorized = {category: [] for category in forbidden_phrases_map.keys()}

        if not response or "results" not in response:
            return detected_phrases_categorized

        alternatives = response.get("results", {}).get("channels", [{}])[0].get("alternatives", [{}])
        if not alternatives: return detected_phrases_categorized

        search_results = alternatives[0].get("search", [])
        if search_results:
            for hit_group in search_results: # search results are per query (keyword)
                phrase_text = hit_group["query"]
                for category, phrases_in_category in forbidden_phrases_map.items():
                    if phrase_text in phrases_in_category:
                        for hit in hit_group.get("hits", []):
                            detected_phrases_categorized[category].append({
                                "phrase": phrase_text,
                                "start": hit.get("start"),
                                "end": hit.get("end"),
                                "confidence": hit.get("confidence"),
                                "snippet": hit.get("snippet", "")[:100] + "..."
                            })
                        break # Phrase found in a category, move to next hit_group
        return detected_phrases_categorized

    def dg_func_detect_forbidden_phrases_manually(self, transcript, forbidden_phrases_map):
        """
        Manually detect forbidden phrases in transcript as a fallback.
        """
        manual_results = {category: [] for category in forbidden_phrases_map.keys()}
        transcript_lower = transcript.lower()
        for category, phrases_in_category in forbidden_phrases_map.items():
            for phrase in phrases_in_category:
                phrase_lower = phrase.lower()
                # Find all occurrences with start and end positions
                for match in re.finditer(re.escape(phrase_lower), transcript_lower):
                    manual_results[category].append({
                        "phrase": phrase, 
                        "start": match.start(), 
                        "end": match.end(), 
                        "manual_detection": True
                    })
        return manual_results

    def dg_func_calculate_risk_score(self, detected_phrases_by_category):
        """
        Calculate risk score based on detected forbidden phrases.
        """
        category_weights = {
            'financial_promises': 0.3,
            'misleading_claims': 0.25,
            'unauthorized_disclosures': 0.2,
            'discriminatory_language': 0.15,
            'unauthorized_offers': 0.1
        }
        total_weighted_score = 0
        total_occurrences = 0
        category_scores_normalized = {}

        for category, phrases_found_list in detected_phrases_by_category.items():
            occurrences_in_category = len(phrases_found_list)
            total_occurrences += occurrences_in_category
            
            normalized_score = 0
            if occurrences_in_category > 0:
                if occurrences_in_category <= 2: normalized_score = 50
                else: normalized_score = 100 # Max score for category if more than 2 occurrences
            
            category_scores_normalized[category] = normalized_score
            total_weighted_score += normalized_score * category_weights.get(category, 0)

        risk_level = "Low"
        if total_weighted_score >= 50: risk_level = "High"
        elif total_weighted_score >= 20: risk_level = "Medium"

        return {
            "overall_risk_score": total_weighted_score,
            "risk_level": risk_level,
            "total_forbidden_occurrences": total_occurrences,
            "category_risk_scores_normalized": category_scores_normalized
        }

    async def main(self, dg_response_json_str, fileid, local_audio_path=None, forbidden_phrases_map=None, **kwargs):
        """
        Main function for detecting forbidden phrases.
        Args:
            dg_response_json_str (str): JSON string of the Deepgram transcription response (can be None).
            fileid (str): The file ID from the orchestrator.
            local_audio_path (str, optional): Path to the audio file for transcription if dg_response_json_str is None.
            forbidden_phrases_map (dict, optional): Custom map of forbidden phrases. Defaults to DEFAULT_FORBIDDEN_PHRASES.
        Returns:
            dict: Results including detected forbidden phrases and risk score.
        """
        print(f"Starting Forbidden Phrase Detection for fileid: {fileid}")
        _phrases_map = forbidden_phrases_map if forbidden_phrases_map is not None else DEFAULT_FORBIDDEN_PHRASES
        all_phrases_flat_list = list(set(phrase for sublist in _phrases_map.values() for phrase in sublist))

        results_payload = {
            "fileid": fileid,
            "audio_file_path": local_audio_path,
            "detected_language": "Unknown",
            "detected_forbidden_phrases_by_category": {category: [] for category in _phrases_map.keys()},
            "risk_score_details": None,
            "raw_transcript_snippet_for_log": None,
            "status": "Error",
            "error": None
        }
        raw_transcript = ""

        try:
            transcription_response = None
            if dg_response_json_str:
                print(f"Using provided transcription for Forbidden Phrases, fileid: {fileid}")
                transcription_response = json.loads(dg_response_json_str)
            elif local_audio_path:
                print(f"Transcribing {local_audio_path} for Forbidden Phrases, fileid: {fileid}")
                transcription_response = await self.dg_func_transcribe_audio_for_phrases(local_audio_path, all_phrases_flat_list)
            else:
                raise ValueError("No Deepgram response string or local audio path provided for Forbidden Phrases.")

            if not transcription_response:
                results_payload["error"] = "Transcription failed or produced no response."
                print(results_payload["error"])
                return results_payload

            raw_transcript, detected_language = self.dg_func_extract_transcript_and_language(transcription_response)
            results_payload["detected_language"] = detected_language
            results_payload["raw_transcript_snippet_for_log"] = raw_transcript[:1000] if raw_transcript else None

            if not raw_transcript:
                results_payload["error"] = "No transcript extracted from Deepgram response."
                print(results_payload["error"])
                return results_payload

            detected_phrases_dg = self.dg_func_extract_detected_phrases_from_dg(transcription_response, _phrases_map)
            found_by_dg = any(bool(ph_list) for ph_list in detected_phrases_dg.values())

            if not found_by_dg and raw_transcript:
                print(f"Deepgram phrase detection yielded no results for {fileid}. Attempting manual scan...")
                detected_phrases_manual = self.dg_func_detect_forbidden_phrases_manually(raw_transcript, _phrases_map)
                if any(bool(ph_list) for ph_list in detected_phrases_manual.values()):
                    print(f"Manual scan found forbidden phrases for {fileid}.")
                    # Merge results: DG results take precedence if any, else manual.
                    # For simplicity, if DG found nothing, use manual. If DG found something, use DG.
                    # A more complex merge could be done if needed.
                    results_payload["detected_forbidden_phrases_by_category"] = detected_phrases_manual
                else:
                    results_payload["detected_forbidden_phrases_by_category"] = detected_phrases_dg # Still use DG empty if manual also empty
            else:
                 results_payload["detected_forbidden_phrases_by_category"] = detected_phrases_dg
            
            risk_score_details = self.dg_func_calculate_risk_score(results_payload["detected_forbidden_phrases_by_category"])
            results_payload["risk_score_details"] = risk_score_details
            results_payload["status"] = "Success"

        except Exception as e_main:
            error_msg = f"Error in DgClassForbiddenPhrases main for fileid {fileid}: {e_main}"
            print(error_msg)
            results_payload["status"] = "Error"
            results_payload["error"] = error_msg
        
        # SQL Logging Block
        if self.sql_helper:
            try:
                fileid_val = results_payload.get("fileid")
                # Log each detected phrase individually if SP is designed that way, or log JSON summary.
                # Assuming DG_LogForbiddenPhrases takes a JSON summary of all detected phrases.
                detected_phrases_json_val = json.dumps(results_payload.get("detected_forbidden_phrases_by_category")) if results_payload.get("detected_forbidden_phrases_by_category") else None
                risk_score_val = results_payload.get("risk_score_details", {}).get("overall_risk_score")
                risk_level_val = results_payload.get("risk_score_details", {}).get("risk_level")
                transcript_snippet_val = results_payload.get("raw_transcript_snippet_for_log")
                status_for_log = results_payload.get("status", "Error")
                error_for_log = results_payload.get("error")

                params = (
                    fileid_val,
                    detected_phrases_json_val,
                    risk_score_val,
                    risk_level_val,
                    transcript_snippet_val,
                    status_for_log,
                    error_for_log
                )
                self.sql_helper.execute_sp("DG_LogForbiddenPhrases", params)
                print(f"Forbidden Phrases results for fileid: {fileid_val} logged to SQL with status: {status_for_log}.")
            except Exception as e_sql:
                sql_error_msg = f"Error logging Forbidden Phrases results to SQL for fileid {fileid_val}: {e_sql}"
                print(sql_error_msg)
                results_payload["status_sql_logging"] = "Error_SQLLogging"
                results_payload["error_sql_logging"] = sql_error_msg
        else:
            print(f"SQL Helper not available. Skipping SQL logging for fileid: {fileid}.")

        print(f"Forbidden Phrase Detection processing completed for fileid: {fileid}. Final status: {results_payload.get('status')}")
        return results_payload

# Example usage (for testing purposes)
async def example_run():
    DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "YOUR_DG_API_KEY_PLACEHOLDER")
    if DEEPGRAM_API_KEY == "YOUR_DG_API_KEY_PLACEHOLDER":
        print("Warning: DEEPGRAM_API_KEY not set. Live API calls will likely fail.")

    class DummySQLHelper:
        def execute_sp(self, sp_name, params):
            print(f"[DummySQLHelper] Called {sp_name} with params: {params}")

    dummy_sql_helper = DummySQLHelper()
    detector = DgClassForbiddenPhrases(deepgram_api_key=DEEPGRAM_API_KEY, sql_helper=dummy_sql_helper)
    example_fileid = "forbidden_test_001"

    dummy_audio_path = "/home/ubuntu/dummy_forbidden_audio.wav"
    if not os.path.exists(dummy_audio_path):
        try:
            import wave
            with wave.open(dummy_audio_path, "w") as wf:
                wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(16000)
                wf.writeframes(b"\x00\x00" * 16000 * 5) # 5 seconds of silence
            print(f"Created dummy audio file: {dummy_audio_path}")
        except Exception as e_wave:
            print(f"Could not create dummy audio file: {e_wave}. Forbidden phrase test with live call might fail.")

    # Test 1: Using a dummy dg_response_json_str
    print("\n--- Test 1: Using dummy_dg_response_json_str ---")
    dummy_response_str = json.dumps({
        "results": {
            "channels": [{
                "detected_language": "en",
                "alternatives": [{
                    "transcript": "This is a guaranteed profit, don't tell anyone. Those people are not like us.",
                    "search": [
                        {"query": "guaranteed profit", "hits": [{"start": 1.0, "end": 2.0, "confidence": 0.9, "snippet": "is a guaranteed profit"}]},
                        {"query": "don't tell anyone", "hits": [{"start": 2.5, "end": 3.5, "confidence": 0.9, "snippet": "profit, don't tell anyone"}]},
                        {"query": "those people", "hits": [{"start": 4.0, "end": 4.5, "confidence": 0.9, "snippet": "anyone. Those people are"}]}
                    ]
                }]
            }]
        }
    })
    results1 = await detector.main(dg_response_json_str=dummy_response_str, fileid=example_fileid)
    if results1:
        print(f"Status: {results1.get("status")}")
        if results1.get("error"): print(f"Error: {results1.get("error")}")
        risk_details = results1.get("risk_score_details", {})
        print(f"Risk Score: {risk_details.get("overall_risk_score", 0):.1f}, Level: {risk_details.get("risk_level", "N/A")}")
        # print("Detected Phrases:", json.dumps(results1.get("detected_forbidden_phrases_by_category"), indent=2))

    # Test 2: Using local_audio_path (will make a live call if API key is valid)
    # print("\n--- Test 2: Using local_audio_path (Live API Call) ---")
    # results2 = await detector.main(dg_response_json_str=None, fileid="forbidden_test_002", local_audio_path=dummy_audio_path)
    # if results2:
    #     print(f"Status: {results2.get("status")}")
    #     if results2.get("error"): print(f"Error: {results2.get("error")}")
    #     risk_details_2 = results2.get("risk_score_details", {})
    #     print(f"Risk Score: {risk_details_2.get("overall_risk_score", 0):.1f}, Level: {risk_details_2.get("risk_level", "N/A")}")

if __name__ == "__main__":
    # import nest_asyncio
    # nest_asyncio.apply()
    asyncio.run(example_run())


