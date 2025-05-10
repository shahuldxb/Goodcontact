import type { Express, Request, Response } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { deepgramService } from "./services/deepgram";
import { getSourceFiles, getProcessedFiles, moveFileToProcessed } from "./services/azure-storage";
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

          // Process the file with Deepgram
          const result = await deepgramService.processAudioFile(filename);
          
          // Move the file from source to processed container
          await moveFileToProcessed(filename);
          
          // Update the asset record
          await storage.updateAsset(fileid, {
            status: 'completed',
            processedDate: new Date()
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
