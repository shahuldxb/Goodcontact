#!/usr/bin/env python3
"""
Simple Flask application to test Azure SQL database connection
"""
from flask import Flask, jsonify
import os
import pymssql
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

# Database connection parameters
AZURE_SQL_SERVER = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
AZURE_SQL_DATABASE = os.environ.get("AZURE_SQL_DATABASE", "call")
AZURE_SQL_USER = os.environ.get("AZURE_SQL_USER", "shahul")
AZURE_SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")

@app.route('/health', methods=['GET'])
def health_check():
    """Basic health check"""
    return jsonify({"status": "ok", "timestamp": str(datetime.now())})

@app.route('/db/health', methods=['GET'])
def db_health_check():
    """Database health check"""
    try:
        # Connect to database
        conn = pymssql.connect(
            server=AZURE_SQL_SERVER,
            database=AZURE_SQL_DATABASE,
            user=AZURE_SQL_USER,
            password=AZURE_SQL_PASSWORD,
            tds_version='7.3',
            port=1433
        )
        
        cursor = conn.cursor()
        
        # Test basic query
        cursor.execute("SELECT 1 AS result")
        result = cursor.fetchone()[0]
        
        # Check tables
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME IN ('rdt_asset', 'rdt_paragraphs', 'rdt_sentences')
        """)
        table_count = cursor.fetchone()[0]
        
        # Close connection
        conn.close()
        
        return jsonify({
            "status": "success",
            "message": "Database connection successful",
            "query_result": result,
            "tables_found": table_count,
            "timestamp": str(datetime.now())
        })
    
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Database connection failed: {str(e)}",
            "timestamp": str(datetime.now())
        }), 500

if __name__ == "__main__":
    # Run Flask app
    logger.info(f"Starting Flask app to test Azure SQL connection")
    logger.info(f"Database: {AZURE_SQL_SERVER}/{AZURE_SQL_DATABASE}")
    app.run(host="0.0.0.0", port=5002)