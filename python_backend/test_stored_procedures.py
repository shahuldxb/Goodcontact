#!/usr/bin/env python3
"""
Test script to verify that the stored procedures are working correctly.
"""

import datetime
import pymssql

def main():
    """Test stored procedure calls."""
    # Connect to database
    conn = pymssql.connect(
        server='callcenter1.database.windows.net',
        port=1433,
        database='call',
        user='shahul',
        password='apple123!@#',
        as_dict=True
    )
    
    cursor = conn.cursor()
    
    try:
        # Test fileid
        fileid = f'test_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
        print(f"Using test file ID: {fileid}")
        
        # Insert asset
        print("Inserting asset record...")
        cursor.execute("""
            INSERT INTO rdt_assets (
                fileid, 
                filename, 
                source_path, 
                destination_path, 
                file_size,
                upload_date,
                status,
                created_dt
            )
            VALUES (
                %s, 
                %s, 
                %s, 
                %s, 
                %s,
                %s,
                %s,
                %s
            )
        """, (
            fileid,
            f"test_file_{fileid}.mp3",
            "shahulin",
            None,
            1024,
            datetime.datetime.now(),
            "completed",
            datetime.datetime.now()
        ))
        print("Asset record inserted successfully")
        
        # Insert metadata
        print("Testing RDS_InsertAudioMetadata...")
        cursor.execute("""
            EXEC RDS_InsertAudioMetadata
            @fileid = %s,
            @request_id = %s,
            @sha256 = %s,
            @created_timestamp = %s,
            @audio_duration = %s,
            @confidence = %s,
            @status = %s
        """, (
            fileid,
            f"request_test_{fileid}",
            "0000000000000000000000000000000000000000000000000000000000000000",
            datetime.datetime.now().isoformat(),
            60.0,  # 60 seconds duration
            0.95,  # 95% confidence
            "completed"
        ))
        print("Metadata inserted successfully")
        
        # Insert paragraph and get paragraph_id
        print("Testing RDS_InsertParagraph...")
        cursor.execute("""
            DECLARE @paragraph_id INT;
            EXEC RDS_InsertParagraph
                @fileid = %s,
                @paragraph_idx = %s,
                @text = %s,
                @start_time = %s,
                @end_time = %s,
                @speaker = %s,
                @num_words = %s,
                @paragraph_id = @paragraph_id OUTPUT;
            SELECT @paragraph_id AS paragraph_id;
        """, (
            fileid,
            1,  # paragraph_idx
            "This is a test paragraph for database procedure testing.",
            0.0,  # Start time in seconds
            10.0,  # End time in seconds
            "1",  # Speaker as string
            9     # Word count
        ))
        
        result = cursor.fetchone()
        if not result:
            print("Error: Failed to fetch paragraph_id")
            return
        
        paragraph_id = result['paragraph_id'] 
        print(f"Paragraph inserted successfully with ID: {paragraph_id}")
        
        # Insert sentence
        print("Testing RDS_InsertSentence...")
        cursor.execute("""
            EXEC RDS_InsertSentence
            @fileid = %s,
            @paragraph_id = %s,
            @sentence_idx = %s,
            @text = %s,
            @start_time = %s,
            @end_time = %s
        """, (
            fileid,
            paragraph_id,  # Paragraph ID from previous insert
            "1",          # Sentence index as string
            "This is a test sentence.",
            0.0,  # Start time in seconds
            5.0   # End time in seconds
        ))
        print("Sentence inserted successfully")
        
        # Commit the transaction
        conn.commit()
        print("All tests completed successfully!")
        
    except Exception as e:
        print(f"Error during testing: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()