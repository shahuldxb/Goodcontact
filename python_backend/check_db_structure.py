#!/usr/bin/env python3
"""
Script to check database structure.
"""

import pymssql

def main():
    """Check database structure."""
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
        # Check rdt_assets table
        print("rdt_assets table columns:")
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'rdt_assets' 
            ORDER BY ORDINAL_POSITION
        """)
        for row in cursor:
            print(f"{row['COLUMN_NAME']:20} {row['DATA_TYPE']}")
        
        # Check if there's a stored procedure to insert assets
        print("\nStored procedures for assets:")
        cursor.execute("""
            SELECT ROUTINE_NAME 
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_TYPE = 'PROCEDURE' 
            AND ROUTINE_NAME LIKE '%Asset%'
        """)
        for row in cursor:
            print(row['ROUTINE_NAME'])
            
        # Check RDS stored procedures
        print("\nRDS stored procedures:")
        cursor.execute("""
            SELECT ROUTINE_NAME 
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_TYPE = 'PROCEDURE' 
            AND ROUTINE_NAME LIKE 'RDS_%'
            ORDER BY ROUTINE_NAME
        """)
        for row in cursor:
            print(row['ROUTINE_NAME'])
            
        # Check a sample of data in rdt_assets table
        print("\nSample data from rdt_assets (first 3 rows):")
        cursor.execute("SELECT TOP 3 * FROM rdt_assets")
        for row in cursor:
            print(f"fileid: {row.get('fileid')}, filename: {row.get('filename')}")
            
    except Exception as e:
        print(f"Error checking database structure: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()