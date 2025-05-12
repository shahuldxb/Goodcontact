#!/usr/bin/env python3
"""
Check if file_size is correctly saved in the database
"""
import os
import pymssql
import logging

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

def check_file_sizes():
    """Check file_size values in the database"""
    conn = connect_to_database()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Check the file with fileid 'file_size_test_1'
        cursor.execute(
            "SELECT id, fileid, filename, file_size FROM rdt_assets WHERE fileid = %s",
            ('file_size_test_1',)
        )
        file_size_test = cursor.fetchone()
        
        # Also check the previous test file
        cursor.execute(
            "SELECT id, fileid, filename, file_size FROM rdt_assets WHERE fileid = %s",
            ('real_test_mudarabah_1',)
        )
        real_test = cursor.fetchone()
        
        # Check the most recent 5 records
        cursor.execute(
            "SELECT TOP 5 id, fileid, filename, file_size FROM rdt_assets ORDER BY id DESC"
        )
        recent_records = cursor.fetchall()
        
        conn.close()
        
        # Display the results
        if file_size_test:
            logger.info(f"File size test record: ID={file_size_test[0]}, FileID={file_size_test[1]}, Filename={file_size_test[2]}, Size={file_size_test[3]}")
        else:
            logger.warning("File size test record not found")
            
        if real_test:
            logger.info(f"Real test record: ID={real_test[0]}, FileID={real_test[1]}, Filename={real_test[2]}, Size={real_test[3]}")
        else:
            logger.warning("Real test record not found")
        
        logger.info("Recent records:")
        for record in recent_records:
            logger.info(f"ID={record[0]}, FileID={record[1]}, Filename={record[2]}, Size={record[3]}")
        
        return True
    except Exception as e:
        logger.error(f"Error checking file sizes: {str(e)}")
        return False

if __name__ == "__main__":
    check_file_sizes()