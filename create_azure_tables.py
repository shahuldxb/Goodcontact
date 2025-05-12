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

def create_azure_tables():
    """
    Create tables required for the application in Azure SQL Server.
    """
    # Get database connection parameters from environment variables
    server = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
    database = os.environ.get("AZURE_SQL_DATABASE", "call")
    user = os.environ.get("AZURE_SQL_USER", "shahul")
    password = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
    
    # If using connection string format
    connection_string = os.environ.get("AZURE_SQL_CONNECTION_STRING") or os.environ.get("DATABASE_URL", "")
    
    logger.info("Creating Azure SQL tables...")
    
    # Display connection details (without sensitive info)
    logger.info(f"Azure SQL Server: {server}")
    logger.info(f"Azure SQL Database: {database}")
    
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
        cursor = conn.cursor()
        
        # Check if rdt_asset table exists
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'rdt_asset'
        """)
        
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            logger.info("Creating rdt_asset table...")
            
            # Create rdt_asset table
            cursor.execute("""
                CREATE TABLE rdt_asset (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    fileid VARCHAR(255) NOT NULL UNIQUE,
                    filename VARCHAR(255) NOT NULL,
                    source_container VARCHAR(100),
                    source_path VARCHAR(255),
                    destination_container VARCHAR(100),
                    destination_path VARCHAR(255),
                    processing_time FLOAT,
                    transcription NVARCHAR(MAX),
                    status VARCHAR(50),
                    created_at DATETIME,
                    updated_at DATETIME
                )
            """)
            
            conn.commit()
            logger.info("rdt_asset table created successfully")
        else:
            logger.info("rdt_asset table already exists")
        
        # Check if rdt_paragraphs table exists
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'rdt_paragraphs'
        """)
        
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            logger.info("Creating rdt_paragraphs table...")
            
            # Create rdt_paragraphs table
            cursor.execute("""
                CREATE TABLE rdt_paragraphs (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    fileid VARCHAR(255) NOT NULL,
                    paragraph_index INT NOT NULL,
                    start_time FLOAT,
                    end_time FLOAT,
                    text NVARCHAR(MAX),
                    speaker VARCHAR(50),
                    sentiment VARCHAR(50),
                    created_at DATETIME,
                    CONSTRAINT FK_paragraphs_asset FOREIGN KEY (fileid) REFERENCES rdt_asset(fileid)
                )
            """)
            
            conn.commit()
            logger.info("rdt_paragraphs table created successfully")
        else:
            logger.info("rdt_paragraphs table already exists")
        
        # Check if rdt_sentences table exists
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'rdt_sentences'
        """)
        
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            logger.info("Creating rdt_sentences table...")
            
            # Create rdt_sentences table
            cursor.execute("""
                CREATE TABLE rdt_sentences (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    fileid VARCHAR(255) NOT NULL,
                    paragraph_id INT,
                    sentence_index INT NOT NULL,
                    start_time FLOAT,
                    end_time FLOAT,
                    text NVARCHAR(MAX),
                    speaker VARCHAR(50),
                    sentiment VARCHAR(50),
                    created_at DATETIME,
                    CONSTRAINT FK_sentences_asset FOREIGN KEY (fileid) REFERENCES rdt_asset(fileid),
                    CONSTRAINT FK_sentences_paragraph FOREIGN KEY (paragraph_id) REFERENCES rdt_paragraphs(id)
                )
            """)
            
            conn.commit()
            logger.info("rdt_sentences table created successfully")
        else:
            logger.info("rdt_sentences table already exists")
        
        # Close connection
        conn.close()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Azure SQL tables creation completed in {elapsed_time:.2f} seconds")
        return True, "Azure SQL tables created successfully"
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Error creating Azure SQL tables: {str(e)}")
        logger.error(f"Failed after {elapsed_time:.2f} seconds")
        return False, f"Table creation failed: {str(e)}"

if __name__ == "__main__":
    success, message = create_azure_tables()
    print(f"Table creation status: {'SUCCESS' if success else 'FAILED'}")
    print(f"Message: {message}")