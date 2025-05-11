import type { Express, Request, Response } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { pythonProxy } from "./services/python-proxy";
import { createPythonProxyMiddleware } from "./services/python-proxy";
import { deepgramService } from "./services/deepgram";
import { 
  getSourceFiles, 
  getProcessedFiles, 
  moveFileToProcessed,
  generateSourceBlobSasUrl,
  generateDestinationBlobSasUrl,
  createContainerIfNotExists
} from "./services/azure-storage";
import { z } from "zod";
import { nanoid } from "nanoid";

export async function registerRoutes(app: Express): Promise<Server> {
  // Get Azure source files (shahulin container)
  app.get("/api/files/source", async (req: Request, res: Response) => {
    try {
      const files = await getSourceFiles();
      res.json({ files });
    } catch (error) {
      console.error("Error fetching source files:", error);
      res.status(500).json({ error: "Failed to fetch source files" });
    }
  });
  
  // Get SAS URL for source file
  app.get("/api/files/source/:filename/sas", async (req: Request, res: Response) => {
    try {
      const { filename } = req.params;
      const expiryHours = req.query.expiry ? Number(req.query.expiry) : 1;
      
      // Generate SAS URL
      const sasUrl = generateSourceBlobSasUrl(filename, expiryHours);
      
      res.json({ 
        filename,
        sasUrl,
        expiryHours
      });
    } catch (error) {
      console.error(`Error generating SAS URL for ${req.params.filename}:`, error);
      res.status(500).json({ error: "Failed to generate SAS URL" });
    }
  });

  // Get Azure processed files (shahulout container)
  app.get("/api/files/processed", async (req: Request, res: Response) => {
    try {
      const files = await getProcessedFiles();
      res.json({ files });
    } catch (error) {
      console.error("Error fetching processed files:", error);
      res.status(500).json({ error: "Failed to fetch processed files" });
    }
  });
  
  // Get SAS URL for processed file
  app.get("/api/files/processed/:filename/sas", async (req: Request, res: Response) => {
    try {
      const { filename } = req.params;
      const expiryHours = req.query.expiry ? Number(req.query.expiry) : 1;
      
      // Generate SAS URL
      const sasUrl = generateDestinationBlobSasUrl(filename, expiryHours);
      
      res.json({ 
        filename,
        sasUrl,
        expiryHours
      });
    } catch (error) {
      console.error(`Error generating SAS URL for ${req.params.filename}:`, error);
      res.status(500).json({ error: "Failed to generate SAS URL" });
    }
  });
  
  // Ensure container exists endpoint
  app.post("/api/containers/ensure", async (req: Request, res: Response) => {
    try {
      const { containerName } = req.body;
      
      if (!containerName) {
        return res.status(400).json({ error: "Container name is required" });
      }
      
      const result = await createContainerIfNotExists(containerName);
      
      res.json({ 
        containerName,
        created: result
      });
    } catch (error) {
      console.error(`Error ensuring container ${req.body.containerName}:`, String(error));
      res.status(500).json({ error: "Failed to ensure container exists" });
    }
  });

  // Process files with Deepgram
  app.post("/api/files/process", async (req: Request, res: Response) => {
    try {
      const schema = z.object({
        files: z.array(z.string())
      });

      const { files } = schema.parse(req.body);
      
      if (files.length === 0) {
        return res.status(400).json({ error: "No files selected for processing" });
      }

      // Process each file sequentially
      const results = [];
      for (const filename of files) {
        try {
          // Create an initial asset record
          const fileid = nanoid();
          await storage.createAsset({
            fileid,
            filename,
            sourcePath: `demoout/${filename}`,
            destinationPath: `shahulout/${filename}`,
            fileSize: 0, // Will be updated after processing
            status: 'processing'
          });

          // Record start time
          const processingStartTime = Date.now();
          
          // Process the file with Deepgram
          console.log(`Sending file ${filename} with ID ${fileid} to Deepgram for processing...`);
          const result = await deepgramService.processAudioFile(filename, fileid);
          
          // Verify we have a valid result before proceeding
          if (!result || !result.success) {
            console.error(`Transcription failed for ${filename}: ${result?.error || 'Unknown error'}`);
            throw new Error(`Transcription failed: ${result?.error || 'Unknown error'}`);
          }
          
          console.log(`RESULT FROM DEEPGRAM PROCESSING: ${JSON.stringify(result, null, 2)}`);
          
          // Debug output for transcription
          if (result && result.transcription) {
            console.log(`Transcription data type: ${typeof result.transcription}`);
            if (typeof result.transcription === 'object') {
              console.log(`Transcription object keys: ${Object.keys(result.transcription).join(', ')}`);
            } else {
              console.log(`Transcription value: ${result.transcription}`);
            }
          } else {
            console.log(`No transcription data available in result: ${JSON.stringify(result)}`);
            throw new Error('No transcription data available in result');
          }
          
          // Verify transcript exists and is valid
          const transcript = result.transcript || '';
          if (!transcript || typeof transcript !== 'string' || transcript.trim().length === 0) {
            console.error(`Invalid or empty transcript for ${filename}`);
            throw new Error('Invalid or empty transcript');
          }
          
          // Calculate processing duration
          const processingDuration = Date.now() - processingStartTime;
          
          try {
            // Import our direct database methods
            const { updateTranscriptionRecord, createAssetRecord, checkRecordExists } = await import('./services/direct-sql');
            
            // Check if record exists, if not, create it
            const exists = await checkRecordExists(fileid);
            if (!exists) {
              console.log(`Creating new asset record for ${fileid}`);
              await createAssetRecord(
                fileid,
                filename,
                `shahulin/${filename}`,
                0 // We don't have the file size, but it's not critical
              );
            }
            
            // Update the record with transcription data
            const updated = await updateTranscriptionRecord(
              fileid,
              transcript,
              result.transcription,
              result.transcription?.results?.metadata?.detected_language || 'English',
              Math.round(processingDuration / 1000) // Convert to seconds
            );
            
            if (updated) {
              console.log(`Successfully updated asset record in SQL Server database for ${fileid}`);
              
              // Also update in-memory storage
              await storage.updateAsset(fileid, {
                status: 'completed',
                processedDate: new Date(),
                processingDuration: Math.round(processingDuration / 1000),
                transcription: transcript,
                transcriptionJson: result.transcription,
                languageDetected: result.transcription?.results?.metadata?.detected_language || 'English'
              });
              
              // Move the file from source to processed container AFTER database update
              await moveFileToProcessed(filename);
              console.log(`Successfully moved file ${filename} to processed container`);
            } else {
              console.error(`Failed to update asset record in PostgreSQL database for ${fileid}`);
              throw new Error('Failed to update record in PostgreSQL database');
            }
          } catch (dbError) {
            console.error(`Database error updating asset record: ${dbError}`);
            throw new Error(`Failed to update database: ${dbError.message || 'Unknown error'}`);
          }
          
          results.push({ fileid, filename, status: 'success' });
        } catch (error) {
          console.error(`Error processing file ${filename}:`, error);
          results.push({ filename, status: 'error', error: error.message });
        }
      }

      res.json({ results });
    } catch (error) {
      console.error("Error processing files:", error);
      res.status(500).json({ error: "Failed to process files" });
    }
  });

  // Get analysis results for a file
  app.get("/api/analysis/:fileid", async (req: Request, res: Response) => {
    try {
      const { fileid } = req.params;
      
      if (!fileid) {
        return res.status(400).json({ error: "File ID is required" });
      }

      const asset = await storage.getAssetById(fileid);
      
      if (!asset) {
        return res.status(404).json({ error: "Asset not found" });
      }

      const results = await storage.getAnalysisResults(fileid);
      res.json({ results });
    } catch (error) {
      console.error("Error fetching analysis results:", error);
      res.status(500).json({ error: "Failed to fetch analysis results" });
    }
  });

  // Get dashboard statistics
  app.get("/api/stats", async (req: Request, res: Response) => {
    try {
      const sourceFiles = await getSourceFiles();
      const processedFiles = await getProcessedFiles();
      
      // Calculate statistics
      const totalFiles = sourceFiles.length + processedFiles.length;
      const processedCount = processedFiles.length;
      const processingTime = 4.2; // Mock value, would be calculated from actual data
      const flaggedCalls = 28; // Mock value, would be calculated from forbidden phrases analysis
      
      res.json({
        totalFiles,
        processedCount,
        processingTime,
        flaggedCalls
      });
    } catch (error) {
      console.error("Error fetching statistics:", error);
      res.status(500).json({ error: "Failed to fetch statistics" });
    }
  });

  // Get sentiment analysis distribution
  app.get("/api/stats/sentiment", async (req: Request, res: Response) => {
    try {
      // Mock data, would be calculated from actual sentiment analysis results
      res.json({
        positive: 48,
        neutral: 32,
        negative: 20
      });
    } catch (error) {
      console.error("Error fetching sentiment distribution:", error);
      res.status(500).json({ error: "Failed to fetch sentiment distribution" });
    }
  });

  // Get topic distribution
  app.get("/api/stats/topics", async (req: Request, res: Response) => {
    try {
      // Mock data, would be calculated from actual topic modeling results
      res.json([
        { name: "Technical Support", percentage: 32 },
        { name: "Billing Issues", percentage: 28 },
        { name: "Product Information", percentage: 18 },
        { name: "Account Management", percentage: 12 },
        { name: "Others", percentage: 10 }
      ]);
    } catch (error) {
      console.error("Error fetching topic distribution:", error);
      res.status(500).json({ error: "Failed to fetch topic distribution" });
    }
  });

  // Get transcription method configuration
  app.get("/api/config/transcription-method", createPythonProxyMiddleware('/config/transcription-method'));
  
  // Update transcription method configuration
  app.post("/api/config/transcription-method", createPythonProxyMiddleware('/config/transcription-method', 'POST'));

  // Debug endpoints
  app.get("/api/debug/direct-transcriptions", createPythonProxyMiddleware('/debug/direct-transcriptions'));
  app.get("/api/debug/direct-transcription", createPythonProxyMiddleware('/debug/direct-transcription'));
  app.post("/api/debug/direct-transcription-upload", createPythonProxyMiddleware('/debug/direct-transcription-upload', 'POST'));
  app.get("/api/debug/direct-test-results", createPythonProxyMiddleware('/debug/direct-test-results'));
  app.get("/api/debug/direct-test-result", createPythonProxyMiddleware('/debug/direct-test-result'));
  
  // Add an endpoint to check assets in database
  app.get("/api/debug/assets", async (req: Request, res: Response) => {
    try {
      const assets = await storage.getAllAssets();
      res.json({ assets });
    } catch (error) {
      console.error("Error fetching assets:", error);
      res.status(500).json({ error: "Failed to fetch assets from database" });
    }
  });

  // Speaker diarization endpoint
  app.post('/api/speaker-diarization', async (req: Request, res: Response) => {
    try {
      const { fileid } = req.body;
      
      if (!fileid) {
        return res.status(400).json({ error: 'Missing required field: fileid' });
      }
      
      // Get the file details from storage
      const fileAsset = await storage.getAsset(fileid);
      
      if (!fileAsset) {
        return res.status(404).json({ error: `File with ID ${fileid} not found` });
      }
      
      console.log(`Processing speaker diarization for file ${fileAsset.filename} (${fileid})`);
      
      try {
        // Call the Python backend for direct diarization
        const response = await pythonProxy.post('/direct/speaker-diarization', {
          filename: fileAsset.filename,
          fileid: fileid
        });
        
        // Extract the diarization result
        const diarizationResult = response.diarization;
        
        if (!diarizationResult || !diarizationResult.success) {
          const errorMessage = diarizationResult?.error || 'Unknown error in speaker diarization';
          console.error(`Speaker diarization failed for ${fileid}: ${errorMessage}`);
          return res.status(400).json({ 
            error: `Speaker diarization failed: ${errorMessage}`,
            fileid: fileid
          });
        }
        
        // Extract speaker data
        const { speaker_count, speakers, speaker_segments, speaker_stats } = diarizationResult;
        
        // Update speaker diarization record in database
        try {
          // Prepare the speaker metrics JSON
          const speakerMetrics = {
            speakers,
            speakerStats: speaker_stats,
            segments: speaker_segments.map(segment => ({
              speaker: segment.speaker,
              text: segment.text,
              startTime: segment.start_time,
              endTime: segment.end_time,
              duration: segment.duration
            }))
          };
          
          // Insert or update speaker diarization data
          await storage.insertOrUpdateSpeakerDiarization({
            fileid,
            status: 'completed',
            created_dt: new Date(),
            created_by: 1, // System user
            speakerCount: speaker_count,
            speakerMetrics: speakerMetrics
          });
          
          console.log(`Successfully stored speaker diarization data for ${fileid}`);
          
          // Return success response
          res.json({
            success: true,
            fileid,
            speakerCount: speaker_count,
            speakers,
            speakerStats: speaker_stats,
            formattedTranscript: diarizationResult.formatted_transcript,
            segmentsCount: speaker_segments.length
          });
          
        } catch (dbError) {
          console.error(`Database error storing speaker diarization: ${dbError}`);
          res.status(500).json({ 
            error: `Failed to store speaker diarization in database: ${dbError instanceof Error ? dbError.message : 'Unknown error'}`,
            fileid
          });
        }
        
      } catch (error) {
        console.error(`Error during speaker diarization processing: ${error}`);
        res.status(500).json({ 
          error: `Speaker diarization processing failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
          fileid
        });
      }
      
    } catch (error) {
      console.error("Error in speaker diarization endpoint:", error);
      res.status(500).json({ error: "Internal server error during speaker diarization" });
    }
  });

  const httpServer = createServer(app);
  return httpServer;
}
