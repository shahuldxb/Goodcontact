#!/usr/bin/env python3
"""
Script to upload an MP3 test audio file to Azure Blob Storage with better format validation
"""
import os
import tempfile
import logging
import time
from azure_storage_service import AzureStorageService
import numpy as np
import subprocess

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_valid_mp3():
    """Create a valid MP3 test file using WAV with explicit content type"""
    try:
        # Create temporary directory for our file
        temp_dir = tempfile.mkdtemp()
        wav_path = os.path.join(temp_dir, "test_audio.wav")
        mp3_path = os.path.join(temp_dir, "test_audio.mp3")
        
        # Generate a 3-second sine wave tone
        sample_rate = 16000
        duration = 3  # seconds
        t = np.linspace(0, duration, sample_rate * duration)
        
        # Create multiple superimposed sine waves for a richer sound
        audio_signal = np.sin(2 * np.pi * 440 * t)  # A4 note (440 Hz)
        audio_signal += 0.5 * np.sin(2 * np.pi * 880 * t)  # A5 note (one octave higher)
        audio_signal += 0.25 * np.sin(2 * np.pi * 1320 * t)  # E5 note (a fifth above A4)
        
        # Add some amplitude variation to simulate speech patterns
        envelope = 0.5 + 0.5 * np.sin(2 * np.pi * 0.5 * t)
        audio_signal = audio_signal * envelope
        
        # Normalize to prevent clipping
        audio_signal = audio_signal / np.max(np.abs(audio_signal)) * 0.9
        
        # Scale to int16 range
        audio_signal = (audio_signal * 32767).astype(np.int16)
        
        # Save as WAV first
        import wave
        with wave.open(wav_path, 'wb') as wf:
            wf.setnchannels(1)  # Mono
            wf.setsampwidth(2)  # 16-bit
            wf.setframerate(sample_rate)
            wf.writeframes(audio_signal.tobytes())
        
        logger.info(f"Created WAV file at {wav_path} ({os.path.getsize(wav_path)} bytes)")
        
        # Convert to MP3 using ffmpeg if available
        try:
            subprocess.run(
                ["ffmpeg", "-i", wav_path, "-codec:a", "libmp3lame", "-qscale:a", "2", mp3_path],
                check=True,
                capture_output=True
            )
            logger.info(f"Converted to MP3 at {mp3_path} ({os.path.getsize(mp3_path)} bytes)")
            return mp3_path
        except (subprocess.SubprocessError, FileNotFoundError) as e:
            logger.warning(f"Could not convert to MP3 using ffmpeg: {str(e)}")
            logger.info("Falling back to WAV file")
            return wav_path
            
    except Exception as e:
        logger.error(f"Error creating audio file: {str(e)}")
        return None

def upload_test_audio():
    """Upload a valid test audio file to Azure Storage"""
    try:
        # Create a valid audio file
        audio_path = create_valid_mp3()
        if not audio_path:
            logger.error("Failed to create test audio file")
            return False
            
        # Get file format
        file_format = "mp3" if audio_path.endswith(".mp3") else "wav"
        
        # Initialize Azure Storage service
        storage_service = AzureStorageService()
        
        # Upload to source container
        blob_name = f"test_audio_{file_format}_{os.path.basename(audio_path)}"
        
        # Upload the file with explicit content type
        content_type = "audio/mpeg" if file_format == "mp3" else "audio/wav"
        
        with open(audio_path, "rb") as file:
            file_data = file.read()
            logger.info(f"Uploading {len(file_data)} bytes to blob '{blob_name}' with content type '{content_type}'")
            
            # Upload to Azure Blob Storage
            blob_client = storage_service.source_container_client.get_blob_client(blob_name)
            blob_client.upload_blob(file_data, overwrite=True, content_type=content_type)
            
            logger.info(f"Successfully uploaded test audio file to '{blob_name}'")
            
            # Check that it's actually in the container
            source_blobs = storage_service.list_source_blobs()
            if blob_name in [blob['name'] for blob in source_blobs]:
                logger.info(f"Verified blob exists in source container: {blob_name}")
            else:
                logger.warning(f"Blob upload succeeded but not found in container listing")
                
            # Return the blob name for further processing
            return blob_name
            
    except Exception as e:
        logger.error(f"Error uploading test audio: {str(e)}")
        return None
    finally:
        # Clean up the local file
        if 'audio_path' in locals() and audio_path and os.path.exists(audio_path):
            os.remove(audio_path)
            logger.info(f"Removed temporary file: {audio_path}")

if __name__ == "__main__":
    blob_name = upload_test_audio()
    if blob_name:
        logger.info(f"Test audio uploaded to blob: {blob_name}")
        
        # Try to process this file
        try:
            import requests
            
            # Trigger processing via the API
            api_url = "http://localhost:5001/process"
            fileid = f"test_{int(time.time())}"  # Generate a test file ID
            
            response = requests.post(api_url, json={
                "filename": blob_name,
                "fileid": fileid
            })
            
            if response.status_code == 200:
                logger.info(f"Successfully initiated processing for {blob_name} with ID {fileid}")
                logger.info(f"Response: {response.json()}")
            else:
                logger.error(f"Failed to initiate processing. Status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                
        except Exception as e:
            logger.error(f"Error triggering processing: {str(e)}")
    else:
        logger.error("Failed to upload test audio")