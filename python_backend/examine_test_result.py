#!/usr/bin/env python3
"""
Script to examine the test result file and check for sentence components
and additional attributes.
"""
import json
import sys
import os

# Test result file path
result_file = "direct_test_results/azure_shortcut_test_test_shortcut_1746977081.json"

try:
    # Load the result file
    with open(result_file, 'r') as f:
        data = json.load(f)
    
    # Print basic information
    print("Test Result Summary:")
    print(f"Timestamp: {data.get('timestamp')}")
    print(f"File: {data.get('file')}")
    print(f"Status: {data.get('status')}")
    print(f"Transcription time: {data.get('elapsed_seconds', 0):.2f} seconds")
    print()
    
    # Get the transcription result
    transcription = data.get('transcription_result', {})
    
    # Check metadata
    print("Metadata:")
    metadata = transcription.get('metadata', {})
    for key, value in metadata.items():
        print(f"- {key}: {value}")
    print()
    
    # Check for additional fields in results
    results = transcription.get('results', {})
    print("Results contains:")
    for key in results.keys():
        print(f"- {key}")
    print()
    
    # Check for paragraphs/sentences
    paragraphs = results.get('paragraphs', [])
    print(f"Paragraphs found: {len(paragraphs)}")
    
    if paragraphs:
        print("\nSample paragraph structure:")
        paragraph = paragraphs[0]
        for key, value in paragraph.items():
            if key == 'words':
                print(f"- {key}: [{len(value)} words]")
            else:
                print(f"- {key}: {value}")
    
    # Check for additional utterances or sentence information
    utterances = results.get('utterances', [])
    print(f"\nUtterances found: {len(utterances)}")
    
    if utterances:
        print("\nSample utterance structure:")
        utterance = utterances[0]
        for key, value in utterance.items():
            if key == 'words':
                print(f"- {key}: [{len(value)} words]")
            else:
                print(f"- {key}: {value}")
    
except FileNotFoundError:
    print(f"Error: File {result_file} not found")
    sys.exit(1)
except json.JSONDecodeError:
    print(f"Error: Could not parse JSON data in {result_file}")
    sys.exit(1)
except Exception as e:
    print(f"Error: {str(e)}")
    sys.exit(1)