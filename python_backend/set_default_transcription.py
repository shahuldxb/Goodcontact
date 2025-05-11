#!/usr/bin/env python3
"""
Script to set the default transcription method to 'shortcut'

This script updates the environment variable DEEPGRAM_TRANSCRIPTION_METHOD
in the current session and also writes it to a .env file for persistence.
"""
import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def set_default_transcription_method(method='shortcut'):
    """
    Set the default transcription method in the environment and .env file.
    
    Args:
        method (str): The transcription method to set as default. 
                     Supported values: 'sdk', 'rest_api', 'direct', 'shortcut', 'enhanced'
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Validate the method
        valid_methods = ['sdk', 'rest_api', 'direct', 'shortcut', 'enhanced']
        if method not in valid_methods:
            logger.error(f"Invalid method: {method}. Must be one of {valid_methods}")
            return False
            
        # Update environment variable for current session
        os.environ["DEEPGRAM_TRANSCRIPTION_METHOD"] = method
        logger.info(f"Set environment variable DEEPGRAM_TRANSCRIPTION_METHOD={method} for current session")
        
        # Create or update .env file in the root directory
        env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
        
        # Read existing file if it exists
        env_vars = {}
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key.strip()] = value.strip().strip('"\'')
        
        # Update or add our variable
        env_vars["DEEPGRAM_TRANSCRIPTION_METHOD"] = method
        
        # Write back to file
        with open(env_file, 'w') as f:
            for key, value in env_vars.items():
                f.write(f"{key}={value}\n")
                
        logger.info(f"Updated .env file at {env_file} with DEEPGRAM_TRANSCRIPTION_METHOD={method}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error setting default transcription method: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    logger.info("Setting default transcription method to 'shortcut'")
    
    # Get method from command line if provided
    method = 'shortcut'
    if len(sys.argv) > 1:
        method = sys.argv[1]
        
    success = set_default_transcription_method(method)
    
    if success:
        logger.info(f"Successfully set default transcription method to '{method}'")
        sys.exit(0)
    else:
        logger.error(f"Failed to set default transcription method to '{method}'")
        sys.exit(1)