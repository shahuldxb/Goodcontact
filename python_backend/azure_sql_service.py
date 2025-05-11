import pymssql
import logging
import json
import os
from datetime import datetime

class AzureSQLService:
    def __init__(self):
        """Initialize the Azure SQL Service"""
        self.logger = logging.getLogger(__name__)
        
        # Azure SQL Server configuration
        self.server = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
        self.database = os.environ.get("AZURE_SQL_DATABASE", "call")
        self.username = os.environ.get("AZURE_SQL_USERNAME", "shahul")
        self.password = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
        self.port = int(os.environ.get("AZURE_SQL_PORT", "1433"))
        
        # Test the connection
        try:
            self._get_connection()
            self.logger.info("Azure SQL Service initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing Azure SQL Service: {str(e)}")
            raise
    
    def _get_connection(self):
        """Get a connection to the Azure SQL Server"""
        try:
            conn = pymssql.connect(
                server=self.server,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                tds_version='7.4',
                as_dict=True
            )
            return conn
        except Exception as e:
            self.logger.error(f"Error connecting to Azure SQL Server: {str(e)}")
            raise
    
    def get_analysis_results(self, fileid):
        """Get analysis results for a file"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            results = {
                "fileid": fileid,
                "asset": None,
                "sentiment": None,
                "language": None,
                "summarization": None,
                "forbiddenPhrases": None,
                "topicModeling": None,
                "speakerDiarization": None,
                "speakerSegments": [],
                "forbiddenPhraseDetails": []
            }
            
            # Get asset data
            cursor.execute("""
                SELECT * FROM rdt_assets WHERE fileid = %s
            """, (fileid,))
            asset = cursor.fetchone()
            if asset:
                results["asset"] = {
                    "fileid": asset["fileid"],
                    "filename": asset["filename"],
                    "sourcePath": asset["source_path"],
                    "destinationPath": asset["destination_path"],
                    "fileSize": asset["file_size"],
                    "uploadDate": asset["upload_date"].isoformat() if asset["upload_date"] else None,
                    "processedDate": asset["processed_date"].isoformat() if asset["processed_date"] else None,
                    "transcription": asset["transcription"],
                    "language": asset["language_detected"],
                    "status": asset["status"],
                    "processingDuration": asset["processing_duration"]
                }
            
            # Get sentiment analysis
            cursor.execute("""
                SELECT * FROM rdt_sentiment WHERE fileid = %s
            """, (fileid,))
            sentiment = cursor.fetchone()
            if sentiment:
                results["sentiment"] = {
                    "overallSentiment": sentiment["overall_sentiment"],
                    "confidenceScore": sentiment["confidence_score"],
                    "sentimentBySegment": sentiment["sentiment_by_segment"] if "sentiment_by_segment" in sentiment else None
                }
            
            # Get language detection
            cursor.execute("""
                SELECT * FROM rdt_language WHERE fileid = %s
            """, (fileid,))
            language = cursor.fetchone()
            if language:
                results["language"] = {
                    "language": language["language"],
                    "confidence": language["confidence"]
                }
            
            # Get summarization
            cursor.execute("""
                SELECT * FROM rdt_summarization WHERE fileid = %s
            """, (fileid,))
            summarization = cursor.fetchone()
            if summarization:
                results["summarization"] = {
                    "summary": summarization["summary"],
                    "keyPoints": summarization["key_points"],
                    "questions": summarization["questions"],
                    "actionItems": summarization["action_items"]
                }
            
            # Get forbidden phrases
            cursor.execute("""
                SELECT * FROM rdt_forbidden_phrases WHERE fileid = %s
            """, (fileid,))
            forbidden_phrases = cursor.fetchone()
            if forbidden_phrases:
                results["forbiddenPhrases"] = {
                    "riskScore": forbidden_phrases["risk_score"],
                    "riskLevel": forbidden_phrases["risk_level"],
                    "categoriesDetected": forbidden_phrases["categories_detected"]
                }
            
            # Get forbidden phrase details
            cursor.execute("""
                SELECT * FROM rdt_forbidden_phrase_details WHERE fileid = %s
            """, (fileid,))
            forbidden_phrase_details = cursor.fetchall()
            for detail in forbidden_phrase_details:
                results["forbiddenPhraseDetails"].append({
                    "category": detail["category"],
                    "phrase": detail["phrase"],
                    "confidence": detail["confidence"],
                    "startTime": detail["start_time"],
                    "endTime": detail["end_time"],
                    "snippet": detail["snippet"]
                })
            
            # Get topic modeling
            cursor.execute("""
                SELECT * FROM rdt_topic_modeling WHERE fileid = %s
            """, (fileid,))
            topic_modeling = cursor.fetchone()
            if topic_modeling:
                results["topicModeling"] = {
                    "topicsDetected": topic_modeling["topics_detected"]
                }
            
            # Get speaker diarization
            cursor.execute("""
                SELECT * FROM rdt_speaker_diarization WHERE fileid = %s
            """, (fileid,))
            speaker_diarization = cursor.fetchone()
            if speaker_diarization:
                results["speakerDiarization"] = {
                    "speakerCount": speaker_diarization["speaker_count"],
                    "speakerMetrics": speaker_diarization["speaker_metrics"]
                }
            
            # Get speaker segments
            cursor.execute("""
                SELECT * FROM rdt_speaker_segments WHERE fileid = %s ORDER BY start_time
            """, (fileid,))
            speaker_segments = cursor.fetchall()
            for segment in speaker_segments:
                results["speakerSegments"].append({
                    "speakerId": segment["speaker_id"],
                    "text": segment["text"],
                    "startTime": segment["start_time"],
                    "endTime": segment["end_time"]
                })
            
            cursor.close()
            conn.close()
            
            return results
        except Exception as e:
            self.logger.error(f"Error getting analysis results: {str(e)}")
            raise
    
    def get_stats(self):
        """Get overall statistics"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            stats = {
                "totalFiles": 0,
                "processedCount": 0,
                "processingTime": 0,
                "flaggedCalls": 0
            }
            
            # Get total files
            cursor.execute("""
                SELECT COUNT(*) as count FROM rdt_assets
            """)
            total = cursor.fetchone()
            if total:
                stats["totalFiles"] = total["count"]
            
            # Get processed count
            cursor.execute("""
                SELECT COUNT(*) as count FROM rdt_assets WHERE status = 'completed'
            """)
            processed = cursor.fetchone()
            if processed:
                stats["processedCount"] = processed["count"]
            
            # Get average processing time
            cursor.execute("""
                SELECT AVG(processing_duration) as avg_time FROM rdt_assets WHERE processing_duration IS NOT NULL
            """)
            avg_time = cursor.fetchone()
            if avg_time and avg_time["avg_time"]:
                stats["processingTime"] = int(avg_time["avg_time"])
            
            # Get flagged calls count
            cursor.execute("""
                SELECT COUNT(*) as count FROM rdt_forbidden_phrases WHERE risk_level = 'high'
            """)
            flagged = cursor.fetchone()
            if flagged:
                stats["flaggedCalls"] = flagged["count"]
            
            cursor.close()
            conn.close()
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting stats: {str(e)}")
            raise
    
    def get_sentiment_stats(self):
        """Get sentiment statistics"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            stats = {
                "positive": 0,
                "neutral": 0,
                "negative": 0
            }
            
            # Get sentiment counts
            cursor.execute("""
                SELECT overall_sentiment, COUNT(*) as count 
                FROM rdt_sentiment 
                GROUP BY overall_sentiment
            """)
            sentiments = cursor.fetchall()
            for s in sentiments:
                if s["overall_sentiment"].lower() == "positive":
                    stats["positive"] = s["count"]
                elif s["overall_sentiment"].lower() == "neutral":
                    stats["neutral"] = s["count"]
                elif s["overall_sentiment"].lower() == "negative":
                    stats["negative"] = s["count"]
            
            cursor.close()
            conn.close()
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting sentiment stats: {str(e)}")
            raise
    
    def get_topic_stats(self):
        """Get topic statistics"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # This is a simplified approach - in a real implementation, 
            # you would parse the topics_detected JSON and calculate actual percentages
            topics = [
                {"name": "Technical Support", "percentage": 35},
                {"name": "Billing Inquiries", "percentage": 25},
                {"name": "Product Information", "percentage": 20},
                {"name": "Account Management", "percentage": 15},
                {"name": "Other", "percentage": 5}
            ]
            
            cursor.close()
            conn.close()
            
            return topics
        except Exception as e:
            self.logger.error(f"Error getting topic stats: {str(e)}")
            raise