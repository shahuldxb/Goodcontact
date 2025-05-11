#!/usr/bin/env python3
"""
Check transcription data in rdt_assets table
"""

import os
import pymssql

# Azure SQL settings
SQL_SERVER = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
SQL_DATABASE = os.environ.get("AZURE_SQL_DATABASE", "call")
SQL_USER = os.environ.get("AZURE_SQL_USER", "shahul")
SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")

def check_transcription():
    """Check transcription data in the rdt_assets table"""
    try:
        # Connect to the database
        conn = pymssql.connect(
            server=SQL_SERVER,
            user=SQL_USER,
            password=SQL_PASSWORD,
            database=SQL_DATABASE
        )
        
        cursor = conn.cursor()
        
        # Query the test records
        cursor.execute("""
            SELECT TOP 10 
                fileid, 
                filename, 
                transcription 
            FROM rdt_assets 
            WHERE fileid LIKE 'test_%' 
            ORDER BY created_dt DESC
        """)
        
        rows = cursor.fetchall()
        
        if not rows:
            print("No test records found in rdt_assets table")
            return
        
        print(f"Found {len(rows)} test records in rdt_assets table")
        print("-" * 80)
        
        for row in rows:
            fileid, filename, transcription = row
            print(f"File ID: {fileid}")
            print(f"Filename: {filename}")
            
            if transcription:
                print(f"Transcription (first 100 chars): {transcription[:100]}...")
                print(f"Transcription length: {len(transcription)} characters")
            else:
                print("Transcription: None")
            
            print("-" * 80)
        
        conn.close()
    
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    check_transcription()