#!/usr/bin/env python3
"""
Test SQL connection to Azure SQL Database
"""
import os
import pymssql
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure SQL settings
SQL_SERVER = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
SQL_DATABASE = os.environ.get("AZURE_SQL_DATABASE", "call")
SQL_USER = os.environ.get("AZURE_SQL_USER", "shahul")
SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
SQL_PORT = int(os.environ.get("AZURE_SQL_PORT", "1433"))

def test_direct_connection():
    """Test direct connection to Azure SQL with basic parameters"""
    logger.info("Testing direct connection with basic parameters...")
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            database=SQL_DATABASE,
            user=SQL_USER,
            password=SQL_PASSWORD,
            port=SQL_PORT
        )
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        logger.info(f"SQL Server version: {row[0]}")
        conn.close()
        logger.info("Direct connection test SUCCESSFUL")
        return True
    except Exception as e:
        logger.error(f"Direct connection test FAILED: {str(e)}")
        return False

def test_connection_with_tds():
    """Test connection with TDS version specified"""
    logger.info("Testing connection with TDS version...")
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            database=SQL_DATABASE,
            user=SQL_USER,
            password=SQL_PASSWORD,
            port=SQL_PORT,
            tds_version="7.3"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        logger.info(f"SQL Server version: {row[0]}")
        conn.close()
        logger.info("TDS version connection test SUCCESSFUL")
        return True
    except Exception as e:
        logger.error(f"TDS version connection test FAILED: {str(e)}")
        return False

def test_connection_with_driver_options():
    """Test connection with driver options similar to Node.js mssql"""
    logger.info("Testing connection with driver options...")
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            database=SQL_DATABASE,
            user=SQL_USER,
            password=SQL_PASSWORD,
            port=SQL_PORT,
            tds_version="7.3",
            as_dict=True,  # Return results as dictionaries
            appname="PythonTestApp",
            autocommit=False,
            timeout=15  # Connection timeout in seconds
        )
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION AS version")
        row = cursor.fetchone()
        logger.info(f"SQL Server version: {row['version']}")
        conn.close()
        logger.info("Driver options connection test SUCCESSFUL")
        return True
    except Exception as e:
        logger.error(f"Driver options connection test FAILED: {str(e)}")
        return False

def test_with_connection_string():
    """Test connection using connection string format"""
    logger.info("Testing connection with connection string format...")
    # Create a connection string similar to what Node.js might use
    conn_str = f"Server={SQL_SERVER};Database={SQL_DATABASE};User Id={SQL_USER};Password={SQL_PASSWORD};"
    
    # Parse connection string
    parts = conn_str.split(';')
    conn_params = {}
    for part in parts:
        if part and '=' in part:
            key, value = part.split('=', 1)
            conn_params[key.lower()] = value
    
    # Extract parameters
    server = conn_params.get('server', '')
    database = conn_params.get('database', '')
    user = conn_params.get('user id', '')
    password = conn_params.get('password', '')
    
    try:
        conn = pymssql.connect(
            server=server,
            database=database,
            user=user,
            password=password,
            port=SQL_PORT,
            tds_version="7.3"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        logger.info(f"SQL Server version: {row[0]}")
        conn.close()
        logger.info("Connection string format test SUCCESSFUL")
        return True
    except Exception as e:
        logger.error(f"Connection string format test FAILED: {str(e)}")
        return False

def check_environment_variables():
    """Check if environment variables are set correctly"""
    logger.info("Checking environment variables...")
    logger.info(f"SQL_SERVER: {SQL_SERVER}")
    logger.info(f"SQL_DATABASE: {SQL_DATABASE}")
    logger.info(f"SQL_USER: {SQL_USER}")
    logger.info(f"SQL_PASSWORD: {'*****' if SQL_PASSWORD else 'Not set'}")
    logger.info(f"SQL_PORT: {SQL_PORT}")
    
    # Check if any environment variable is missing or empty
    if not SQL_SERVER or not SQL_DATABASE or not SQL_USER or not SQL_PASSWORD:
        logger.error("One or more environment variables are missing or empty")
        return False
    return True

def test_basic_table_operations():
    """Test basic table operations"""
    logger.info("Testing basic table operations...")
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            database=SQL_DATABASE,
            user=SQL_USER,
            password=SQL_PASSWORD,
            port=SQL_PORT,
            tds_version="7.3"
        )
        cursor = conn.cursor()
        
        # Check if rdt_asset table exists
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'rdt_asset'
        """)
        count = cursor.fetchone()[0]
        logger.info(f"rdt_asset table exists: {count > 0}")
        
        # Check if rdt_paragraphs table exists
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'rdt_paragraphs'
        """)
        count = cursor.fetchone()[0]
        logger.info(f"rdt_paragraphs table exists: {count > 0}")
        
        # Check if rdt_sentences table exists
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'rdt_sentences'
        """)
        count = cursor.fetchone()[0]
        logger.info(f"rdt_sentences table exists: {count > 0}")
        
        conn.close()
        logger.info("Basic table operations test SUCCESSFUL")
        return True
    except Exception as e:
        logger.error(f"Basic table operations test FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("===== AZURE SQL CONNECTION TEST SCRIPT =====")
    logger.info("The script will try various connection methods to troubleshoot Azure SQL connectivity")
    
    # Check environment variables
    env_ok = check_environment_variables()
    if not env_ok:
        logger.warning("Environment variables check FAILED, but proceeding with default values")
    
    # Run connection tests
    direct_ok = test_direct_connection()
    tds_ok = test_connection_with_tds()
    driver_ok = test_connection_with_driver_options()
    conn_str_ok = test_with_connection_string()
    table_ok = test_basic_table_operations() if any([direct_ok, tds_ok, driver_ok, conn_str_ok]) else False
    
    # Print summary
    logger.info("\n===== TEST SUMMARY =====")
    logger.info(f"Environment Variables Check: {'PASSED' if env_ok else 'FAILED'}")
    logger.info(f"Direct Connection Test: {'PASSED' if direct_ok else 'FAILED'}")
    logger.info(f"TDS Version Connection Test: {'PASSED' if tds_ok else 'FAILED'}")
    logger.info(f"Driver Options Connection Test: {'PASSED' if driver_ok else 'FAILED'}")
    logger.info(f"Connection String Format Test: {'PASSED' if conn_str_ok else 'FAILED'}")
    logger.info(f"Basic Table Operations Test: {'PASSED' if table_ok else 'FAILED'}")
    
    # Overall result
    if any([direct_ok, tds_ok, driver_ok, conn_str_ok]):
        logger.info("OVERALL RESULT: CONNECTION SUCCESSFUL using at least one method")
    else:
        logger.error("OVERALL RESULT: ALL CONNECTION METHODS FAILED")