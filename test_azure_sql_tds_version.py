#!/usr/bin/env python3
"""
Test Azure SQL Database connection with different TDS versions and configurations
"""
import os
import pymssql
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_azure_sql_connection():
    # Get database connection parameters from environment variables
    server = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
    database = os.environ.get("AZURE_SQL_DATABASE", "call")
    user = os.environ.get("AZURE_SQL_USER", "shahul")
    password = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
    
    logger.info("Testing Azure SQL Server connection with different TDS versions...")
    logger.info(f"Azure SQL Server: {server}")
    logger.info(f"Azure SQL Database: {database}")
    
    # Try different TDS versions
    tds_versions = ['7.0', '7.1', '7.2', '7.3', '7.4']
    
    for tds_version in tds_versions:
        logger.info(f"Attempting connection with TDS version {tds_version}...")
        start_time = time.time()
        
        try:
            # Connect with TDS version
            conn = pymssql.connect(
                server=server,
                database=database,
                user=user,
                password=password,
                tds_version=tds_version,
                port=1433,
                as_dict=True
            )
            
            # Test basic query
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION AS version")
            result = cursor.fetchone()
            
            elapsed_time = time.time() - start_time
            logger.info(f"Successfully connected with TDS version {tds_version} in {elapsed_time:.2f} seconds")
            logger.info(f"SQL Server version: {result['version'][:50]}...")
            
            # Check tables
            cursor.execute("""
                SELECT COUNT(*) AS table_count FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
            """)
            table_count = cursor.fetchone()['table_count']
            logger.info(f"Found {table_count} tables in the database")
            
            # Close connection
            conn.close()
            logger.info(f"Connection with TDS version {tds_version} closed successfully")
            logger.info("=" * 50)
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"Failed to connect with TDS version {tds_version} after {elapsed_time:.2f} seconds")
            logger.error(f"Error: {e}")
            logger.info("=" * 50)

if __name__ == "__main__":
    test_azure_sql_connection()