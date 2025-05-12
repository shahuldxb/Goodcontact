#!/usr/bin/env python3
"""
Test direct SQL connection from the Flask app environment
"""
import os
import sys
import pymssql
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_direct_connection():
    """
    Test direct connection to Azure SQL with hardcoded credentials
    This is a standalone test to rule out environment or configuration issues
    """
    logger.info("Testing direct connection with hardcoded credentials...")
    
    try:
        conn = pymssql.connect(
            server='callcenter1.database.windows.net',
            database='call',
            user='shahul',
            password='apple123!@#',
            port='1433',
            tds_version='7.3'  # Use TDS version which confirmed works in test_sql_connection.py
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        logger.info(f"SQL Server version: {row[0]}")
        
        # Check if required tables exist
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME IN ('rdt_asset', 'rdt_paragraphs', 'rdt_sentences')
        """)
        table_count = cursor.fetchone()[0]
        logger.info(f"Required tables found: {table_count}/3")
        
        conn.close()
        logger.info("Direct connection test SUCCESSFUL")
        return True, "Connection successful"
    except Exception as e:
        logger.error(f"Direct connection test FAILED: {str(e)}")
        return False, str(e)

if __name__ == "__main__":
    success, message = test_direct_connection()
    if success:
        print("Connection test PASSED!")
        sys.exit(0)
    else:
        print(f"Connection test FAILED: {message}")
        sys.exit(1)