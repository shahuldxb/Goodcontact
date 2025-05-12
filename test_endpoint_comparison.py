#!/usr/bin/env python3
"""
Test and compare both transcription endpoints
This script will:
1. Call the original endpoint
2. Call the new v2 endpoint
3. Check the database to compare file_size values
"""
import os
import pymssql
import requests
import json
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure SQL Server connection parameters
AZURE_SQL_SERVER = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
AZURE_SQL_DATABASE = os.environ.get("AZURE_SQL_DATABASE", "call") 
AZURE_SQL_USER = os.environ.get("AZURE_SQL_USER", "shahul")
AZURE_SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")

def connect_to_database():
    """Connect to the Azure SQL database"""
    try:
        conn = pymssql.connect(
            server=AZURE_SQL_SERVER,
            user=AZURE_SQL_USER,
            password=AZURE_SQL_PASSWORD,
            database=AZURE_SQL_DATABASE,
            port=1433,
            tds_version='7.3'
        )
        logger.info("Connected to Azure SQL database successfully")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Azure SQL database: {str(e)}")
        return None

def test_both_endpoints():
    """Test both transcription endpoints and compare results"""
    base_url = "http://localhost:5001"
    original_endpoint = f"{base_url}/direct/transcribe"
    v2_endpoint = f"{base_url}/direct/transcribe_v2"
    
    # Create a unique fileid for each test
    original_fileid = f"original_test_{int(time.time())}"
    v2_fileid = f"v2_test_{int(time.time())}"
    
    # Common test data
    test_file = "business_investment_account_(mudarabah)_neutral.mp3"
    
    logger.info(f"Testing both endpoints with file: {test_file}")
    
    # Test original endpoint
    original_data = {
        "filename": test_file,
        "fileid": original_fileid
    }
    
    try:
        # Call original endpoint
        logger.info(f"Calling original endpoint with {original_fileid}")
        original_response = requests.post(original_endpoint, json=original_data)
        
        if original_response.status_code != 200:
            logger.error(f"Original endpoint error: {original_response.status_code}, {original_response.text}")
            return False
        
        logger.info(f"Original endpoint succeeded")
        
        # Test v2 endpoint
        v2_data = {
            "filename": test_file,
            "fileid": v2_fileid
        }
        
        # Call v2 endpoint
        logger.info(f"Calling v2 endpoint with {v2_fileid}")
        v2_response = requests.post(v2_endpoint, json=v2_data)
        
        if v2_response.status_code != 200:
            logger.error(f"V2 endpoint error: {v2_response.status_code}, {v2_response.text}")
            return False
        
        logger.info(f"V2 endpoint succeeded")
        
        # Wait a moment for transactions to complete
        time.sleep(3)
        
        # Now check the database to compare file_size values
        conn = connect_to_database()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Get original endpoint results
        cursor.execute(
            "SELECT id, fileid, filename, file_size FROM rdt_assets WHERE fileid = %s",
            (original_fileid,)
        )
        original_record = cursor.fetchone()
        
        # Get v2 endpoint results
        cursor.execute(
            "SELECT id, fileid, filename, file_size FROM rdt_assets WHERE fileid = %s",
            (v2_fileid,)
        )
        v2_record = cursor.fetchone()
        
        conn.close()
        
        # Display the results
        if original_record:
            logger.info(f"Original endpoint record: ID={original_record[0]}, FileID={original_record[1]}, Filename={original_record[2]}, Size={original_record[3]}")
        else:
            logger.warning("Original endpoint record not found")
            
        if v2_record:
            logger.info(f"V2 endpoint record: ID={v2_record[0]}, FileID={v2_record[1]}, Filename={v2_record[2]}, Size={v2_record[3]}")
        else:
            logger.warning("V2 endpoint record not found")
        
        # Compare file sizes
        if original_record and v2_record:
            original_size = original_record[3]
            v2_size = v2_record[3]
            
            if original_size == v2_size and original_size > 0:
                logger.info(f"SUCCESS: Both endpoints stored the same non-zero file size: {original_size}")
                return True
            else:
                logger.warning(f"ISSUE: Different file sizes - Original: {original_size}, V2: {v2_size}")
                return False
        
        return True
    
    except Exception as e:
        logger.error(f"Error in test: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_both_endpoints()
    print(f"Test result: {'SUCCESS' if success else 'FAILURE'}")