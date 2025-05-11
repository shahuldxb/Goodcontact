import type { Express, Request, Response } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
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
            sourcePath: `shahulin/${filename}`,
            destinationPath: `shahulout/${filename}`,
            fileSize: 0, // Will be updated after processing
            status: 'processing'
          });

          // Record start time
          const processingStartTime = Date.now();
          
          // Process the file with Deepgram
          const result = await deepgramService.processAudioFile(filename, fileid);
          
          // Calculate processing duration
          const processingDuration = Date.now() - processingStartTime;
          
          // Move the file from source to processed container
          await moveFileToProcessed(filename);
          
          // Update the asset record
          await storage.updateAsset(fileid, {
            status: 'completed',
            processedDate: new Date(),
            processingDuration: Math.round(processingDuration / 1000), // Convert to seconds
            // Extract the transcript from the correct structure
            transcription: result.transcription?.result?.utterances?.[0]?.transcript 
              || result.transcription?.result?.channels?.[0]?.alternatives?.[0]?.transcript 
              || '',
            // Save the entire JSON response (stringify + parse to ensure proper serialization)
            transcriptionJson: JSON.parse(JSON.stringify(result.transcription || {})),
            languageDetected: result.transcription?.result?.metadata?.detected_language || 'English'
          });
          
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

  const httpServer = createServer(app);
  return httpServer;
}
