"""
Create missing stored procedures for Deepgram analytics platform
"""
import os
import logging
import pymssql
from azure_sql_service import AzureSQLService

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_dg_log_language_detection_sp(conn):
    """Create the DG_LogLanguageDetection stored procedure"""
    try:
        cursor = conn.cursor()
        
        # Check if the stored procedure already exists
        cursor.execute("""
            SELECT COUNT(*) AS count 
            FROM sys.procedures 
            WHERE name = 'DG_LogLanguageDetection'
        """)
        result = cursor.fetchone()
        
        if result and result['count'] > 0:
            logger.info("Stored procedure DG_LogLanguageDetection already exists")
            return
        
        # Create the stored procedure
        cursor.execute("""
        CREATE PROCEDURE DG_LogLanguageDetection
            @FileID NVARCHAR(255),
            @DgDetectedLanguageCode NVARCHAR(100),
            @DgDetectedLanguageName NVARCHAR(255),
            @DgLanguageConfidence FLOAT,
            @FullTranscriptTranslated NVARCHAR(MAX),
            @TranslationTargetLanguage NVARCHAR(100),
            @TranslationError NVARCHAR(MAX),
            @TextSegmentLanguagesJson NVARCHAR(MAX),
            @TranslatedSpeakerSegmentsJson NVARCHAR(MAX),
            @Status NVARCHAR(50)
        AS
        BEGIN
            -- First insert/update the main language results
            IF EXISTS (SELECT * FROM rdt_language WHERE fileid = @FileID)
            BEGIN
                UPDATE rdt_language
                SET 
                    language = @DgDetectedLanguageCode,
                    confidence = @DgLanguageConfidence,
                    status = @Status
                WHERE fileid = @FileID
            END
            ELSE
            BEGIN
                INSERT INTO rdt_language (fileid, language, confidence, status)
                VALUES (@FileID, @DgDetectedLanguageCode, @DgLanguageConfidence, @Status)
            END
            
            -- Then insert/update detailed translation data in a separate table if needed
            -- Additional logic for future detailed translation storage can be added here
        END
        """)
        
        conn.commit()
        logger.info("Created stored procedure: DG_LogLanguageDetection")
    except Exception as e:
        logger.error(f"Error creating stored procedure DG_LogLanguageDetection: {str(e)}")
        raise

def main():
    """Main function to create missing stored procedures"""
    try:
        # Get SQL connection
        sql_service = AzureSQLService()
        conn = sql_service._get_connection()
        
        # Create stored procedures
        create_dg_log_language_detection_sp(conn)
        
        # Close connection
        conn.close()
        
        logger.info("All missing stored procedures created successfully")
    except Exception as e:
        logger.error(f"Error creating missing stored procedures: {str(e)}")
        raise

if __name__ == "__main__":
    main()