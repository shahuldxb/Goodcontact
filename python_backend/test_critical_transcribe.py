#!/usr/bin/env python3
"""
Test script for dg_class_critical_transcribe_rest with a minimal blob SAS URL.
"""
import json
from dg_class_critical_transcribe_rest import DgClassCriticalTranscribeRest

# Use the provided Deepgram API key
DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"

# SAS URL for the audio blob
AUDIO_URL = "https://infolder.blob.core.windows.net/shahulin/agricultural_finance_(murabaha)_angry.mp3?sp=r&st=2025-05-11T14:30:26Z&se=2025-11-12T22:30:26Z&spr=https&sv=2024-11-04&sr=b&sig=q2gumh51pXiVFgidPda5JQJXvGWwF4z%2BhE2tI9Ahkm0%3D"

def main():
    """
    Main function to test the critical transcription with URL.
    """
    print("Initializing DgClassCriticalTranscribeRest...")
    transcriber = DgClassCriticalTranscribeRest(DEEPGRAM_API_KEY)
    
    print(f"Starting transcription of audio from URL with model nova-3...")
    result = transcriber.transcribe_with_url(
        audio_url=AUDIO_URL,
        model="nova-3",  # Use the latest model as requested
        diarize=True,    # Enable speaker diarization
        debug_mode=True  # Save debugging information
    )
    
    # Check if the transcription was successful
    if result['success']:
        print("Transcription successful!")
        
        # Save the full response to a JSON file
        with open('critical_transcribe_result.json', 'w') as f:
            json.dump(result, f, indent=2)
        print(f"Full result saved to critical_transcribe_result.json")
        
        # Extract and display basic transcript information
        if 'full_response' in result and 'results' in result['full_response']:
            if 'channels' in result['full_response']['results']:
                channels = result['full_response']['results']['channels']
                if channels and len(channels) > 0 and 'alternatives' in channels[0]:
                    transcript = channels[0]['alternatives'][0].get('transcript', 'No transcript found')
                    print("\nTranscript snippet (first 200 chars):")
                    print(transcript[:200] + "..." if len(transcript) > 200 else transcript)
                    
                    # Get word confidence if available
                    if 'confidence' in channels[0]['alternatives'][0]:
                        confidence = channels[0]['alternatives'][0]['confidence']
                        print(f"\nOverall confidence: {confidence:.4f}")
        
        # Check for speaker diarization
        if 'utterances' in result['full_response'].get('results', {}):
            utterances = result['full_response']['results']['utterances']
            if utterances:
                print(f"\nFound {len(utterances)} utterances with speaker diarization")
                # Print first utterance as example
                if len(utterances) > 0:
                    first_utterance = utterances[0]
                    print(f"Speaker {first_utterance.get('speaker', '?')}: {first_utterance.get('text', '')[:100]}...")
    else:
        print(f"Transcription failed: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main()