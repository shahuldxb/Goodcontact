#!/usr/bin/env python3
"""
A simple script to execute the test_direct_transcription function
and save its output to a file for inspection.
"""
import os
import json
import sys
import time
from datetime import datetime

# Import the test function
from test_direct_transcription import test_direct_transcription

# Specify the output directory with absolute path
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "direct_test_results")

def extract_transcript(result):
    """
    Extract a formatted transcript from the Deepgram result.
    
    Args:
        result: The Deepgram result object
        
    Returns:
        str: A formatted transcript with speaker information
    """
    if not result or "results" not in result:
        return "No transcript available"
        
    try:
        utterances = result.get("results", {}).get("utterances", [])
        
        if not utterances:
            return "No utterances found in the transcript"
            
        transcript_parts = []
        current_speaker = None
        
        for utterance in utterances:
            speaker = utterance.get("speaker")
            text = utterance.get("transcript", "")
            
            # Only add speaker label when speaker changes
            if speaker != current_speaker:
                if speaker == 0:
                    transcript_parts.append("\n[Customer]: ")
                else:
                    transcript_parts.append("\n[Agent]: ")
                current_speaker = speaker
                
            transcript_parts.append(text + " ")
            
        return "".join(transcript_parts)
    except Exception as e:
        return f"Error extracting transcript: {str(e)}"

def run_test(blob_name):
    """
    Run the test_direct_transcription function and save the output.
    
    Args:
        blob_name: Name of the blob in Azure Storage to process
        
    Returns:
        dict: Result from test_direct_transcription
    """
    print(f"Running test_direct_transcription for {blob_name}...")
    start_time = time.time()
    
    # Call the test function
    result = test_direct_transcription(blob_name=blob_name)
    
    # Calculate execution time
    elapsed_time = time.time() - start_time
    print(f"Test completed in {elapsed_time:.2f} seconds")
    
    # Extract and format the transcript
    formatted_transcript = extract_transcript(result)
    
    # Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    # Save result to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_DIR, f"{os.path.splitext(blob_name)[0]}_{timestamp}.json")
    
    output_data = {
        "blob_name": blob_name,
        "timestamp": datetime.now().isoformat(),
        "execution_time_seconds": elapsed_time,
        "formatted_transcript": formatted_transcript,
        "result": result
    }
    
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)
    
    print(f"Result saved to {output_file}")
    
    # Print out a sample of the formatted transcript
    print("\nSample of formatted transcript:")
    print(formatted_transcript[:500] + "..." if len(formatted_transcript) > 500 else formatted_transcript)
    
    return result

if __name__ == "__main__":
    # Get blob name from command line arguments or use default
    blob_name = sys.argv[1] if len(sys.argv) > 1 else "agricultural_leasing_(ijarah)_normal.mp3"
    run_test(blob_name)