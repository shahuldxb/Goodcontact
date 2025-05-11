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
SQL_USER = os.environ.get("AZURE_SQL_USER", "shahul")  # Make sure this matches the environment variable name in Node.js
SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
SQL_PORT = int(os.environ.get("AZURE_SQL_PORT", "1433"))

def test_sql_connection():
    """Test connection to Azure SQL Database"""
    logger.info(f"Testing connection to Azure SQL Database: {SQL_SERVER}/{SQL_DATABASE}")
    
    try:
        # Connect to SQL Database
        conn = pymssql.connect(
            server=SQL_SERVER,
            port=SQL_PORT,
            database=SQL_DATABASE,
            user=SQL_USER,
            password=SQL_PASSWORD,
            tds_version='7.4',
            as_dict=True
        )
        logger.info("Connection established successfully!")
        
        # Get tables
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        tables = cursor.fetchall()
        
        # Display tables
        logger.info(f"Found {len(tables)} tables:")
        for i, table in enumerate(tables):
            logger.info(f"  {i+1}. {table['TABLE_NAME']}")
        
        # Get stored procedures
        cursor.execute("""
            SELECT ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE'
        """)
        procs = cursor.fetchall()
        
        # Display stored procedures
        logger.info(f"Found {len(procs)} stored procedures:")
        for i, proc in enumerate(procs):
            logger.info(f"  {i+1}. {proc['ROUTINE_NAME']}")
        
        # Close connection
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        logger.error(f"Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_sql_connection()