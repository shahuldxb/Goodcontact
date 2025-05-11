#!/usr/bin/env python3
"""
Create WAV files with actual speech content for testing Deepgram
"""
import os
import logging
import numpy as np
import wave
import tempfile

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_speech_like_signal():
    """
    Generate a signal that has speech-like characteristics
    This creates a more complex waveform than a simple sine wave,
    which might be more easily recognized by Deepgram as speech
    """
    # Parameters
    sample_rate = 16000  # Standard speech sample rate
    duration = 2  # seconds
    total_samples = sample_rate * duration
    
    # Generate a frame-by-frame signal with frequency shifts to mimic speech
    signal = np.zeros(total_samples, dtype=np.float32)
    
    # Generate several frequency components that shift over time (like vowels in speech)
    freqs = [240, 500, 1000, 2000]  # Frequencies that commonly occur in speech
    
    for i in range(0, total_samples, sample_rate // 8):
        # Choose random vowel-like frequencies for this frame
        frame_freqs = np.random.choice(freqs, size=3, replace=False)
        frame_length = min(sample_rate // 8, total_samples - i)
        
        # Time array for this frame
        t = np.arange(frame_length) / sample_rate
        
        # Generate a mixture of frequencies with varying amplitudes
        frame = np.zeros(frame_length)
        for freq in frame_freqs:
            # Add a frequency component with random amplitude
            amplitude = np.random.uniform(0.2, 0.8)
            frame += amplitude * np.sin(2 * np.pi * freq * t)
        
        # Add some noise to make it more realistic
        noise = np.random.normal(0, 0.05, frame_length)
        frame += noise
        
        # Apply a simple envelope to the frame (attack and decay)
        envelope = np.ones(frame_length)
        attack_samples = min(int(0.01 * sample_rate), frame_length // 10)
        decay_samples = min(int(0.01 * sample_rate), frame_length // 10)
        
        if attack_samples > 0:
            envelope[:attack_samples] = np.linspace(0, 1, attack_samples)
        if decay_samples > 0 and frame_length - decay_samples > 0:
            envelope[frame_length - decay_samples:] = np.linspace(1, 0, decay_samples)
            
        frame *= envelope
        
        # Add to our signal
        signal[i:i+frame_length] = frame
    
    # Normalize to prevent clipping
    max_val = np.max(np.abs(signal))
    if max_val > 0:
        signal = signal / max_val * 0.9
    
    # Convert to 16-bit PCM
    return (signal * 32767).astype(np.int16)

def save_wav(signal, filename, sample_rate=16000):
    """Save a signal to a WAV file"""
    with wave.open(filename, 'wb') as wav_file:
        # Set parameters
        nchannels = 1  # mono
        sampwidth = 2  # 16-bit
        framerate = sample_rate
        nframes = len(signal)
        comptype = 'NONE'
        compname = 'not compressed'
        
        # Set the parameters
        wav_file.setparams((nchannels, sampwidth, framerate, nframes, comptype, compname))
        
        # Write the frames
        wav_file.writeframes(signal.tobytes())
    
    logger.info(f"Saved WAV file to {filename} ({os.path.getsize(filename)} bytes)")
    return filename

def create_speech_wav():
    """Create a WAV file with speech-like content"""
    try:
        # Create a temp directory for our file
        temp_dir = tempfile.mkdtemp()
        output_path = os.path.join(temp_dir, "speech_test.wav")
        
        # Generate speech-like signal
        logger.info("Generating speech-like signal...")
        signal = generate_speech_like_signal()
        
        # Save to WAV file
        logger.info(f"Saving speech signal to {output_path}...")
        save_wav(signal, output_path)
        
        return output_path
    except Exception as e:
        logger.error(f"Error creating speech WAV file: {str(e)}")
        return None

if __name__ == "__main__":
    filepath = create_speech_wav()
    if filepath:
        logger.info(f"Created speech WAV file at: {filepath}")
        
        # Verify file with basic wave module
        try:
            with wave.open(filepath, 'rb') as w:
                channels = w.getnchannels()
                sample_width = w.getsampwidth()
                frame_rate = w.getframerate()
                frames = w.getnframes()
                logger.info(f"WAV file details: {channels} channels, {sample_width} bytes/sample, "
                          f"{frame_rate} Hz, {frames} frames, {frames/frame_rate:.2f} seconds")
        except Exception as e:
            logger.error(f"Error reading WAV file: {str(e)}")
    else:
        logger.error("Failed to create speech WAV file")