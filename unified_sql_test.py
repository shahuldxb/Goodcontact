#!/usr/bin/env python3
"""
Unified SQL connection test script that tests multiple connection methods
to determine which one works with Azure SQL Server
"""
import os
import pymssql
import logging
import json
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_connection_with_app_code():
    """Test connection using the code from app.py"""
    logger.info("=== Testing connection with app.py code ===")
    server = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
    database = os.environ.get("AZURE_SQL_DATABASE", "call")
    user = os.environ.get("AZURE_SQL_USER", "shahul")
    password = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
    
    # Azure SQL connection parameters (used by the app)
    sql_conn_params = {
        'server': server,
        'database': database,
        'user': user,
        'password': password,
        'tds_version': '7.3',
        'port': 1433
    }
    
    start_time = time.time()
    logger.info(f"Connecting to {server}/{database}...")
    
    try:
        # Test connection using params
        conn = pymssql.connect(**sql_conn_params)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION AS version")
        result = cursor.fetchone()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Successfully connected in {elapsed_time:.2f} seconds")
        logger.info(f"Version: {result[0][:50]}...")
        
        # Test table access
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        table_count = cursor.fetchone()[0]
        logger.info(f"Found {table_count} tables")
        
        # Close connection
        conn.close()
        logger.info("Connection closed successfully")
        return True
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Failed to connect after {elapsed_time:.2f} seconds")
        logger.error(f"Error: {str(e)}")
        return False

def test_connection_with_test_code():
    """Test connection using the code from the test script"""
    logger.info("=== Testing connection with test script code ===")
    server = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
    database = os.environ.get("AZURE_SQL_DATABASE", "call")
    user = os.environ.get("AZURE_SQL_USER", "shahul")
    password = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
    
    start_time = time.time()
    logger.info(f"Connecting to {server}/{database}...")
    
    try:
        # Test connection with TDS version 7.3 (from test script)
        conn = pymssql.connect(
            server=server,
            database=database,
            user=user,
            password=password,
            tds_version='7.3',
            port=1433,
            as_dict=True
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION AS version")
        result = cursor.fetchone()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Successfully connected in {elapsed_time:.2f} seconds")
        logger.info(f"Version: {result['version'][:50]}...")
        
        # Test table access
        cursor.execute("""
            SELECT COUNT(*) AS table_count FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        table_count = cursor.fetchone()['table_count']
        logger.info(f"Found {table_count} tables")
        
        # Close connection
        conn.close()
        logger.info("Connection closed successfully")
        return True
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Failed to connect after {elapsed_time:.2f} seconds")
        logger.error(f"Error: {str(e)}")
        return False

def test_connection_with_node_code():
    """Test connection using approach similar to Node.js code"""
    logger.info("=== Testing connection with Node.js-like code ===")
    server = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
    database = os.environ.get("AZURE_SQL_DATABASE", "call")
    user = os.environ.get("AZURE_SQL_USER", "shahul")
    password = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
    
    start_time = time.time()
    logger.info(f"Connecting to {server}/{database}...")
    
    try:
        # Test connection with approach more like Node.js code
        # Note: pymssql doesn't have exact equivalents for Node.js mssql options
        conn = pymssql.connect(
            server=server,
            database=database,
            user=user,
            password=password,
            tds_version='7.3',
            port=1433
        )
        
        cursor = conn.cursor(as_dict=True)
        cursor.execute("SELECT @@VERSION AS version")
        result = cursor.fetchone()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Successfully connected in {elapsed_time:.2f} seconds")
        logger.info(f"Version: {result['version'][:50]}...")
        
        # Test specific tables
        for table in ['rdt_asset', 'rdt_paragraphs', 'rdt_sentences']:
            cursor.execute(f"""
                SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = '{table}'
            """)
            exists = cursor.fetchone()['count'] > 0
            logger.info(f"Table {table} exists: {exists}")
        
        # Close connection
        conn.close()
        logger.info("Connection closed successfully")
        return True
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Failed to connect after {elapsed_time:.2f} seconds")
        logger.error(f"Error: {str(e)}")
        return False

def test_environment_variables():
    """Test environment variable access"""
    logger.info("=== Testing environment variables ===")
    
    # List all environment variables related to Azure SQL
    for key in os.environ:
        if 'SQL' in key.upper() or 'AZURE' in key.upper():
            # Mask password for security
            value = '****' if 'PASSWORD' in key.upper() else os.environ[key]
            logger.info(f"{key} = {value}")
    
    return True

if __name__ == "__main__":
    logger.info("Starting unified SQL connection test...")
    
    # Test environment variables first
    test_environment_variables()
    
    # Test each connection method
    app_success = test_connection_with_app_code()
    test_success = test_connection_with_test_code()
    node_success = test_connection_with_node_code()
    
    logger.info("\n=== SUMMARY ===")
    logger.info(f"Connection using app code: {'SUCCESS' if app_success else 'FAILED'}")
    logger.info(f"Connection using test code: {'SUCCESS' if test_success else 'FAILED'}")
    logger.info(f"Connection using Node.js-like code: {'SUCCESS' if node_success else 'FAILED'}")
    
    if not (app_success and test_success and node_success):
        logger.error("Some connection methods failed. Review logs for details.")
    else:
        logger.info("All connection methods successful!")