"""
Async Azure SQL Service

This module provides asynchronous database connectivity for Azure SQL Server
using aiopg for greater performance in asyncio contexts.
"""

import os
import logging
import json
import asyncio
import aiopg
import traceback
from datetime import datetime

class AsyncSQLService:
    def __init__(self):
        """Initialize the Async Azure SQL Service"""
        self.logger = logging.getLogger(__name__)
        
        # Azure SQL connection string
        self.host = os.environ.get("PGHOST", "callcenter1.database.windows.net")
        self.user = os.environ.get("PGUSER", "shahul")
        self.password = os.environ.get("PGPASSWORD", "apple123!@#") 
        self.database = os.environ.get("PGDATABASE", "callcenter")
        self.port = os.environ.get("PGPORT", "5432")
        
        # Build DSN string
        self.dsn = f"dbname={self.database} user={self.user} password={self.password} host={self.host} port={self.port}"
        
        self.logger.info("Async Azure SQL Service initialized")

    async def get_connection(self):
        """Get an asynchronous connection to Azure SQL Server"""
        try:
            return await aiopg.connect(dsn=self.dsn)
        except Exception as e:
            self.logger.error(f"Error connecting to Azure SQL: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    async def update_assets_record(self, fileid, data):
        """
        Update an existing record in rdt_assets table asynchronously
        
        Args:
            fileid (str): The file ID to update
            data (dict): Dictionary containing fields to update
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            async with await self.get_connection() as conn:
                async with conn.cursor() as cursor:
                    # Check if record already exists
                    await cursor.execute("SELECT * FROM rdt_assets WHERE fileid = %s", (fileid,))
                    existing_asset = await cursor.fetchone()
                    
                    if existing_asset:
                        # Build the update SQL dynamically based on fields in data
                        fields = []
                        values = []
                        
                        for key, value in data.items():
                            fields.append(f"{key} = %s")
                            values.append(value)
                        
                        # Add fileid at the end of values for the WHERE clause
                        values.append(fileid)
                        
                        # Execute the update
                        sql = f"UPDATE rdt_assets SET {', '.join(fields)} WHERE fileid = %s"
                        await cursor.execute(sql, values)
                        
                        self.logger.info(f"Updated asset record for fileid: {fileid}")
                        return True
                    else:
                        self.logger.warning(f"No existing asset record found for fileid: {fileid}")
                        return False
        except Exception as e:
            self.logger.error(f"Error updating asset record: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    async def insert_assets_record(self, fileid, data):
        """
        Insert a new record into rdt_assets table asynchronously
        
        Args:
            fileid (str): The file ID to insert
            data (dict): Dictionary containing fields to insert
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            async with await self.get_connection() as conn:
                async with conn.cursor() as cursor:
                    # Ensure fileid is included in the data
                    data['fileid'] = fileid
                    
                    # Build the insert SQL dynamically
                    fields = list(data.keys())
                    placeholders = ['%s'] * len(fields)
                    values = [data[field] for field in fields]
                    
                    # Execute the insert
                    sql = f"INSERT INTO rdt_assets ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                    await cursor.execute(sql, values)
                    
                    self.logger.info(f"Inserted new asset record for fileid: {fileid}")
                    return True
        except Exception as e:
            self.logger.error(f"Error inserting asset record: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    async def upsert_assets_record(self, fileid, data):
        """
        Update or insert a record in rdt_assets table asynchronously
        
        Args:
            fileid (str): The file ID to upsert
            data (dict): Dictionary containing fields to upsert
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            async with await self.get_connection() as conn:
                async with conn.cursor() as cursor:
                    # Check if record already exists
                    await cursor.execute("SELECT * FROM rdt_assets WHERE fileid = %s", (fileid,))
                    existing_asset = await cursor.fetchone()
                    
                    if existing_asset:
                        # Build the update SQL dynamically
                        fields = []
                        values = []
                        
                        for key, value in data.items():
                            fields.append(f"{key} = %s")
                            values.append(value)
                        
                        # Add fileid at the end of values for the WHERE clause
                        values.append(fileid)
                        
                        # Execute the update
                        sql = f"UPDATE rdt_assets SET {', '.join(fields)} WHERE fileid = %s"
                        await cursor.execute(sql, values)
                        
                        self.logger.info(f"Updated existing asset record for fileid: {fileid}")
                    else:
                        # Ensure fileid is included in the data
                        data['fileid'] = fileid
                        
                        # Build the insert SQL dynamically
                        fields = list(data.keys())
                        placeholders = ['%s'] * len(fields)
                        values = [data[field] for field in fields]
                        
                        # Execute the insert
                        sql = f"INSERT INTO rdt_assets ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                        await cursor.execute(sql, values)
                        
                        self.logger.info(f"Inserted new asset record for fileid: {fileid}")
                    
                    return True
        except Exception as e:
            self.logger.error(f"Error upserting asset record: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
            
    async def update_asset_status(self, fileid, status, error_message=None):
        """
        Update the status of an asset record asynchronously
        
        Args:
            fileid (str): The file ID to update
            status (str): The new status ('processing', 'completed', 'error')
            error_message (str, optional): Error message to store if status is 'error'
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            data = {'status': status}
            
            if status == 'completed':
                data['processed_date'] = datetime.now()
            
            if status == 'error' and error_message:
                data['error_message'] = error_message
                
            return await self.update_assets_record(fileid, data)
        except Exception as e:
            self.logger.error(f"Error updating asset status: {str(e)}")
            return False
            
    async def store_transcription(self, fileid, transcription_response, transcript_text, detected_language):
        """
        Store transcription data in rdt_assets table asynchronously
        
        Args:
            fileid (str): The file ID
            transcription_response (dict): The full Deepgram response
            transcript_text (str): The extracted transcript text
            detected_language (str): The detected language
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Prepare data for upsert
            data = {
                'transcription': transcript_text,
                'transcription_json': json.dumps(transcription_response),
                'language_detected': detected_language,
                'status': 'processing',
                'created_dt': datetime.now()
            }
            
            return await self.upsert_assets_record(fileid, data)
        except Exception as e:
            self.logger.error(f"Error storing transcription: {str(e)}")
            return False

    async def execute_proc(self, proc_name, params=None):
        """
        Execute a stored procedure asynchronously
        
        Args:
            proc_name (str): Name of the stored procedure
            params (tuple, optional): Parameters for the stored procedure
            
        Returns:
            Any: The result of the stored procedure
        """
        try:
            async with await self.get_connection() as conn:
                async with conn.cursor() as cursor:
                    if params:
                        await cursor.execute(f"EXEC {proc_name} {', '.join(['%s'] * len(params))}", params)
                    else:
                        await cursor.execute(f"EXEC {proc_name}")
                    
                    # Fetch all results
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            self.logger.error(f"Error executing stored procedure {proc_name}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None