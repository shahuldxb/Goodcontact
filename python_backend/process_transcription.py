#!/usr/bin/env python3
"""
Process transcription from direct_transcribe.py
"""

import json
import os
from datetime import datetime
from direct_transcribe import transcribe_url

def process_and_save_transcription(audio_url, output_dir="transcription_results"):
    """
    Process a transcription and analyze/save the results
    
    Args:
        audio_url (str): URL to the audio file
        output_dir (str): Directory to save the results
        
    Returns:
        dict: Processing results and analysis
    """
    print(f"Starting transcription process for audio URL")
    
    # Step 1: Get the transcription from the direct_transcribe module
    transcription_result = transcribe_url(audio_url, model="nova-3", diarize=True)
    
    if not transcription_result["success"]:
        print(f"Transcription failed: {transcription_result.get('error', 'Unknown error')}")
        return {"success": False, "error": transcription_result.get("error", "Unknown error")}
    
    # Step 2: Extract and process the response
    response = transcription_result["response"]
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a timestamp for the output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Extract the filename from the URL for the output file name
    try:
        filename = audio_url.split('/')[-1].split('?')[0]
    except:
        filename = f"transcription_{timestamp}"
    
    # Save the full response to a JSON file
    output_path = f"{output_dir}/{filename}_{timestamp}.json"
    with open(output_path, 'w') as f:
        json.dump(response, f, indent=2)
    
    print(f"Full transcription saved to {output_path}")

    # Step 3: Extract and analyze key information
    analysis_results = {}
    
    try:
        # Extract the basic transcript
        if 'results' in response and 'channels' in response['results']:
            channels = response['results']['channels']
            if channels and len(channels) > 0 and 'alternatives' in channels[0]:
                transcript = channels[0]['alternatives'][0].get('transcript', '')
                confidence = channels[0]['alternatives'][0].get('confidence', 0)
                
                analysis_results["transcript"] = transcript
                analysis_results["confidence"] = confidence
                
                print(f"\nTranscript (first 150 chars): {transcript[:150]}...")
                print(f"Overall confidence: {confidence:.4f}")
        
        # Extract speaker information if available
        speakers_found = False
        
        if 'results' in response and 'utterances' in response['results']:
            utterances = response['results']['utterances']
            if utterances and len(utterances) > 0:
                speakers_found = True
                analysis_results["utterances"] = utterances
                analysis_results["num_utterances"] = len(utterances)
                
                print(f"\nFound {len(utterances)} utterances with speaker information")
                
                # Count the number of unique speakers
                unique_speakers = set()
                for utterance in utterances:
                    unique_speakers.add(utterance.get('speaker', 0))
                
                analysis_results["num_speakers"] = len(unique_speakers)
                print(f"Detected {len(unique_speakers)} unique speakers")
                
                # Print a sample of speaker utterances
                print("\nSample of speaker utterances:")
                for i, utterance in enumerate(utterances[:3]):  # First 3 utterances
                    speaker = utterance.get('speaker', '?')
                    text = utterance.get('text', '')
                    print(f"Speaker {speaker}: {text[:100]}..." if len(text) > 100 else f"Speaker {speaker}: {text}")
                    
                    if i >= 2:  # Only show first 3
                        break
    
    except Exception as e:
        print(f"Error analyzing transcription: {str(e)}")
        analysis_results["analysis_error"] = str(e)
    
    # Return the combined results
    result = {
        "success": True,
        "output_file": output_path,
        "analysis": analysis_results
    }
    
    return result

if __name__ == "__main__":
    # Process a sample audio file
    audio_url = "https://infolder.blob.core.windows.net/shahulin/agricultural_finance_(murabaha)_angry.mp3?sp=r&st=2025-05-11T14:30:26Z&se=2025-11-12T22:30:26Z&spr=https&sv=2024-11-04&sr=b&sig=q2gumh51pXiVFgidPda5JQJXvGWwF4z%2BhE2tI9Ahkm0%3D"
    result = process_and_save_transcription(audio_url)
    
    print("\nProcessing complete!")
    print(f"Success: {result['success']}")
    if result['success']:
        print(f"Output file: {result['output_file']}")
        print(f"Analysis summary:")
        print(f"- Transcript length: {len(result['analysis'].get('transcript', ''))}")
        print(f"- Confidence: {result['analysis'].get('confidence', 0):.4f}")
        if 'num_speakers' in result['analysis']:
            print(f"- Number of speakers: {result['analysis']['num_speakers']}")
            print(f"- Number of utterances: {result['analysis']['num_utterances']}")