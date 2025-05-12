#!/usr/bin/env python3
"""
Direct SQL Connection Module
Provides direct and reliable SQL connection functionality for Azure SQL
Based on the successful test_direct_sql.py implementation
"""
import os
import pymssql
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DirectSQLConnection:
    """
    Direct SQL Connection class for Azure SQL
    Use the proven working configuration
    """
    
    def __init__(self, server=None, database=None, user=None, password=None):
        """Initialize with connection parameters"""
        self.server = server or os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
        self.database = database or os.environ.get("AZURE_SQL_DATABASE", "call")
        self.user = user or os.environ.get("AZURE_SQL_USER", "shahul")
        self.password = password or os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
    
    def get_connection(self):
        """
        Get SQL connection using the proven method from test_direct_sql.py
        """
        try:
            conn = pymssql.connect(
                server=self.server,
                database=self.database,
                user=self.user,
                password=self.password,
                port='1433',
                tds_version='7.3'  # Use TDS version which confirmed works
            )
            return conn
        except Exception as e:
            logger.error(f"Error connecting to SQL database: {str(e)}")
            raise
    
    def execute_query(self, query, params=None):
        """
        Execute a SQL query with parameters
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            
        Returns:
            list: Query results
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            results = cursor.fetchall()
            conn.commit()
            return results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_non_query(self, query, params=None):
        """
        Execute a non-query SQL statement (INSERT, UPDATE, DELETE)
        
        Args:
            query (str): SQL statement to execute
            params (tuple, optional): Parameters for the statement
            
        Returns:
            int: Number of rows affected
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            affected_rows = cursor.rowcount
            conn.commit()
            return affected_rows
        except Exception as e:
            logger.error(f"Error executing non-query: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def test_connection(self):
        """
        Test the connection to the database
        
        Returns:
            tuple: (success, message)
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            row = cursor.fetchone()
            version = row[0] if row else "Unknown"
            conn.close()
            return True, f"Connection successful. SQL Server version: {version}"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

# Example usage
if __name__ == "__main__":
    sql = DirectSQLConnection()
    success, message = sql.test_connection()
    print(f"Connection test {'passed' if success else 'failed'}: {message}")