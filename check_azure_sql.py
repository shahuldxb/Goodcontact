import os
import logging
import time
from datetime import datetime
import pymssql

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_azure_sql_connection():
    # Get database connection parameters from environment variables
    server = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
    database = os.environ.get("AZURE_SQL_DATABASE", "call")
    user = os.environ.get("AZURE_SQL_USER", "shahul")
    password = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
    
    # If using connection string format
    connection_string = os.environ.get("DATABASE_URL", "")
    
    logger.info("Checking Azure SQL Server connection...")
    
    # Display connection details (without sensitive info)
    logger.info(f"Azure SQL Server: {server}")
    logger.info(f"Azure SQL Database: {database}")
    logger.info(f"Connection string provided: {'Yes' if connection_string else 'No'}")
    
    start_time = time.time()
    
    try:
        # Attempt connection
        if connection_string and ";" in connection_string:
            logger.info("Using connection string for Azure SQL database")
            # Parse connection string format
            # Example format: "Server=server.database.windows.net;Database=db;User Id=user;Password=pass;"
            conn_params = {}
            parts = connection_string.split(';')
            for part in parts:
                if part and '=' in part:
                    key, value = part.split('=', 1)
                    conn_params[key.lower()] = value
            
            server = conn_params.get('server', server)
            database = conn_params.get('database', database)
            user = conn_params.get('user id', user)
            password = conn_params.get('password', password)
        
        # Connect to Azure SQL Server
        logger.info(f"Connecting to Azure SQL Server: {server}, Database: {database}")
        conn = pymssql.connect(server=server, database=database, user=user, password=password)
        
        # If connection successful, check tables
        if conn is not None:
            cursor = conn.cursor()
            
            if db_type == "azuresql":
                # Check Azure SQL tables
                # Check if rdt_asset table exists
                logger.info("Checking Azure SQL tables...")
                cursor.execute("""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = 'rdt_asset'
                """)
                
                table_exists = cursor.fetchone()[0] > 0
                logger.info(f"rdt_asset table exists: {table_exists}")
                
                if table_exists:
                    # Get row count
                    cursor.execute("SELECT COUNT(*) FROM rdt_asset")
                    row_count = cursor.fetchone()[0]
                    logger.info(f"rdt_asset table row count: {row_count}")
                    
                    # Get first record (if any)
                    if row_count > 0:
                        cursor.execute("SELECT TOP 1 fileid, filename, status FROM rdt_asset")
                        sample_row = cursor.fetchone()
                        logger.info(f"Sample record - fileid: {sample_row[0]}, filename: {sample_row[1]}, status: {sample_row[2]}")
                
                # Check if paragraphs and sentences tables exist
                for table in ['rdt_paragraphs', 'rdt_sentences']:
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_NAME = '{table}'
                    """)
                    table_exists = cursor.fetchone()[0] > 0
                    logger.info(f"{table} table exists: {table_exists}")
                    
                    if table_exists:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        row_count = cursor.fetchone()[0]
                        logger.info(f"{table} table row count: {row_count}")
            
            elif db_type == "postgresql":
                # Check PostgreSQL tables
                logger.info("Checking PostgreSQL tables...")
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        AND table_name = 'rdt_asset'
                    )
                """)
                
                table_exists = cursor.fetchone()[0]
                logger.info(f"rdt_asset table exists: {table_exists}")
                
                if table_exists:
                    # Get row count
                    cursor.execute("SELECT COUNT(*) FROM rdt_asset")
                    row_count = cursor.fetchone()[0]
                    logger.info(f"rdt_asset table row count: {row_count}")
                    
                    # Get first record (if any)
                    if row_count > 0:
                        cursor.execute("SELECT fileid, filename, status FROM rdt_asset LIMIT 1")
                        sample_row = cursor.fetchone()
                        logger.info(f"Sample record - fileid: {sample_row[0]}, filename: {sample_row[1]}, status: {sample_row[2]}")
                
                # Check if paragraphs and sentences tables exist
                for table in ['rdt_paragraphs', 'rdt_sentences']:
                    cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public'
                            AND table_name = '{table}'
                        )
                    """)
                    table_exists = cursor.fetchone()[0]
                    logger.info(f"{table} table exists: {table_exists}")
                    
                    if table_exists:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        row_count = cursor.fetchone()[0]
                        logger.info(f"{table} table row count: {row_count}")
            
            # Close connection
            conn.close()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Azure SQL connection check completed successfully in {elapsed_time:.2f} seconds")
        return True, f"Connected successfully to {server}, database {database}"
    
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Azure SQL connection failed: {str(e)}")
        logger.error(f"Failed after {elapsed_time:.2f} seconds")
        return False, f"Connection failed: {str(e)}"

if __name__ == "__main__":
    success, message = check_db_connection()
    print(f"Connection status: {'SUCCESS' if success else 'FAILED'}")
    print(f"Message: {message}")