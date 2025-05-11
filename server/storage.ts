import {
  RdtAsset, InsertRdtAsset,
  RdtSentiment, InsertRdtSentiment,
  RdtLanguage, InsertRdtLanguage,
  RdtSummarization, InsertRdtSummarization,
  RdtForbiddenPhrases, InsertRdtForbiddenPhrases,
  RdtForbiddenPhrasesDetails, InsertRdtForbiddenPhrasesDetails,
  RdtTopicModeling, InsertRdtTopicModeling,
  RdtSpeakerDiarization, InsertRdtSpeakerDiarization,
  RdtSpeakerSegments, InsertRdtSpeakerSegments
} from "@shared/schema";
// import { sql } from "@neondatabase/serverless";
import { nanoid } from 'nanoid';
import { BlobServiceClient, StorageSharedKeyCredential } from "@azure/storage-blob";
import { DgClassSpeakerDiarization } from "./services/deepgram";
import { sqlConnect } from "./services/postgres-sql";

export interface IStorage {
  // RDT Assets methods
  getAssets(): Promise<RdtAsset[]>;
  getAssetById(fileid: string): Promise<RdtAsset | undefined>;
  createAsset(asset: InsertRdtAsset): Promise<RdtAsset>;
  updateAsset(fileid: string, updates: Partial<RdtAsset>): Promise<RdtAsset | undefined>;
  
  // Azure Blob storage methods
  getSourceFiles(): Promise<any[]>;
  getProcessedFiles(): Promise<any[]>;
  moveFileToProcessed(filename: string): Promise<boolean>;
  
  // Analysis methods
  getAnalysisResults(fileid: string): Promise<any>;
  
  // Deepgram transcription methods
  transcribeAndAnalyze(filename: string): Promise<any>;
  
  // Individual analysis methods
  saveSentimentAnalysis(data: InsertRdtSentiment): Promise<RdtSentiment>;
  saveLanguageDetection(data: InsertRdtLanguage): Promise<RdtLanguage>;
  saveSummarization(data: InsertRdtSummarization): Promise<RdtSummarization>;
  saveForbiddenPhrases(data: InsertRdtForbiddenPhrases, details?: InsertRdtForbiddenPhrasesDetails[]): Promise<RdtForbiddenPhrases>;
  saveTopicModeling(data: InsertRdtTopicModeling): Promise<RdtTopicModeling>;
  saveSpeakerDiarization(data: InsertRdtSpeakerDiarization, segments?: InsertRdtSpeakerSegments[]): Promise<RdtSpeakerDiarization>;
  
  // Getting analysis results
  getSentimentAnalysisByFileid(fileid: string): Promise<RdtSentiment | undefined>;
  getLanguageDetectionByFileid(fileid: string): Promise<RdtLanguage | undefined>;
  getSummarizationByFileid(fileid: string): Promise<RdtSummarization | undefined>;
  getForbiddenPhrasesByFileid(fileid: string): Promise<{phrases: RdtForbiddenPhrases, details: RdtForbiddenPhrasesDetails[]} | undefined>;
  getTopicModelingByFileid(fileid: string): Promise<RdtTopicModeling | undefined>;
  getSpeakerDiarizationByFileid(fileid: string): Promise<{diarization: RdtSpeakerDiarization, segments: RdtSpeakerSegments[]} | undefined>;
}

export class MemStorage implements IStorage {
  private assets = new Map<string, RdtAsset>();
  private sentiment = new Map<string, RdtSentiment>();
  private language = new Map<string, RdtLanguage>();
  private summarization = new Map<string, RdtSummarization>();
  private forbiddenPhrases = new Map<string, RdtForbiddenPhrases>();
  private forbiddenPhrasesDetails = new Map<number, RdtForbiddenPhrasesDetails[]>();
  private topicModeling = new Map<string, RdtTopicModeling>();
  private speakerDiarization = new Map<string, RdtSpeakerDiarization>();
  private speakerSegments = new Map<number, RdtSpeakerSegments[]>();
  
  private currentAssetId = 1;
  private currentSentimentId = 1;
  private currentLanguageId = 1;
  private currentSummarizationId = 1;
  private currentForbiddenPhrasesId = 1;
  private currentForbiddenPhrasesDetailsId = 1;
  private currentTopicModelingId = 1;
  private currentSpeakerDiarizationId = 1;
  private currentSpeakerSegmentsId = 1;

  // Mock files for development (will be replaced with Azure Blob Storage in production)
  private sourceFiles = [
    { name: 'call_recording_001.wav', size: 4.2 * 1024 * 1024, lastModified: new Date('2023-06-02'), type: 'audio/wav' },
    { name: 'support_call_002.wav', size: 6.8 * 1024 * 1024, lastModified: new Date('2023-06-01'), type: 'audio/wav' },
    { name: 'customer_complaint_003.wav', size: 5.1 * 1024 * 1024, lastModified: new Date('2023-05-28'), type: 'audio/wav' },
    { name: 'sales_inquiry_004.wav', size: 3.7 * 1024 * 1024, lastModified: new Date('2023-05-25'), type: 'audio/wav' },
    { name: 'technical_support_005.wav', size: 7.2 * 1024 * 1024, lastModified: new Date('2023-05-22'), type: 'audio/wav' },
  ];
  
  private processedFiles = [
    { name: 'old_call_001.wav', size: 3.5 * 1024 * 1024, processedDate: new Date('2023-05-15'), sentiment: 'positive', language: 'English' },
    { name: 'old_call_002.wav', size: 5.2 * 1024 * 1024, processedDate: new Date('2023-05-10'), sentiment: 'neutral', language: 'English' },
  ];

  async getAssets(): Promise<RdtAsset[]> {
    return Array.from(this.assets.values());
  }

  async getAllAssets(): Promise<RdtAsset[]> {
    return Array.from(this.assets.values());
  }

  async getAssetById(fileid: string): Promise<RdtAsset | undefined> {
    return this.assets.get(fileid);
  }

  async createAsset(asset: InsertRdtAsset): Promise<RdtAsset> {
    try {
      // First, store in memory
      const id = this.currentAssetId++;
      const newAsset: RdtAsset = { 
        ...asset, 
        id, 
        created_dt: new Date(),
        created_by: 1
      };
      this.assets.set(asset.fileid, newAsset);
      
      // Then, persist to SQL database
      const { insertRecord } = await import('./services/postgres-sql');
      
      // Convert property names to SQL column names (camelCase to snake_case)
      const sqlData = {
        fileid: asset.fileid,
        filename: asset.filename,
        source_path: asset.sourcePath,
        destination_path: asset.destinationPath,
        file_size: asset.fileSize || 0,
        status: asset.status || 'pending',
        created_by: 1
      };
      
      // Insert into SQL database
      await insertRecord('rdt_assets', sqlData);
      
      console.log(`Asset created in SQL database: ${asset.fileid}`);
      return newAsset;
    } catch (error) {
      console.error('Error creating asset in SQL database:', error);
      // Still return the in-memory asset even if SQL insertion fails
      const inMemoryAsset = this.assets.get(asset.fileid);
      if (inMemoryAsset) return inMemoryAsset;
      throw error;
    }
  }

  async updateAsset(fileid: string, updates: Partial<RdtAsset>): Promise<RdtAsset | undefined> {
    try {
      // First update in memory
      const asset = this.assets.get(fileid);
      if (!asset) return undefined;
      
      const updatedAsset = { ...asset, ...updates };
      this.assets.set(fileid, updatedAsset);
      
      // Then update in SQL database
      const { executeQuery } = await import('./services/postgres-sql');
      
      // Convert camelCase to snake_case for SQL columns with safe values
      const sqlUpdates: Record<string, any> = {};
      if (updates.status) sqlUpdates.status = updates.status;
      if (updates.processedDate) sqlUpdates.processed_date = updates.processedDate;
      
      // Handle text fields safely - limit long strings and ensure they're properly escaped
      if (updates.transcription) {
        // Limit transcription to 4000 chars to prevent SQL errors
        sqlUpdates.transcription = typeof updates.transcription === 'string' 
          ? updates.transcription.substring(0, 4000) 
          : '';
      }
      
      if (updates.transcriptionJson) {
        // Convert transcription JSON to string safely
        try {
          const jsonStr = JSON.stringify(updates.transcriptionJson);
          sqlUpdates.transcription_json = jsonStr.substring(0, 4000);
        } catch (e) {
          console.warn('Error stringifying transcriptionJson:', e);
          sqlUpdates.transcription_json = '{}';
        }
      }
      
      if (updates.fileSize) sqlUpdates.file_size = updates.fileSize;
      
      if (updates.languageDetected) {
        sqlUpdates.language_detected = typeof updates.languageDetected === 'string'
          ? updates.languageDetected.substring(0, 100)
          : 'Unknown';
      }
      
      if (updates.errorMessage) {
        sqlUpdates.error_message = typeof updates.errorMessage === 'string'
          ? updates.errorMessage.substring(0, 1000)
          : '';
      }
      
      if (updates.processingDuration) {
        sqlUpdates.processing_duration = typeof updates.processingDuration === 'number'
          ? updates.processingDuration
          : 0;
      }
      
      // Build SQL update query
      if (Object.keys(sqlUpdates).length > 0) {
        const setClause = Object.entries(sqlUpdates)
          .map(([key, _], index) => `${key} = @param${index}`)
          .join(', ');
        
        const values = [...Object.values(sqlUpdates), fileid];
        
        const query = `
          UPDATE rdt_assets
          SET ${setClause}
          WHERE fileid = @param${Object.values(sqlUpdates).length};
        `;
        
        await executeQuery(query, values);
        console.log(`Asset updated in SQL database: ${fileid}`);
      }
      
      return updatedAsset;
    } catch (error) {
      console.error('Error updating asset in SQL database:', error);
      // Still return the in-memory asset even if SQL update fails
      return this.assets.get(fileid);
    }
  }

  async getSourceFiles(): Promise<any[]> {
    // This would be implemented with Azure Blob Storage client
    return this.sourceFiles;
  }

  async getProcessedFiles(): Promise<any[]> {
    // This would be implemented with Azure Blob Storage client
    return this.processedFiles;
  }

  async moveFileToProcessed(filename: string): Promise<boolean> {
    // This would be implemented with Azure Blob Storage client
    const fileIndex = this.sourceFiles.findIndex(f => f.name === filename);
    if (fileIndex === -1) return false;
    
    const file = this.sourceFiles[fileIndex];
    this.processedFiles.push({
      name: file.name,
      size: file.size,
      processedDate: new Date(),
      sentiment: ['positive', 'neutral', 'negative'][Math.floor(Math.random() * 3)],
      language: 'English'
    });
    
    this.sourceFiles.splice(fileIndex, 1);
    return true;
  }

  async getAnalysisResults(fileid: string): Promise<any> {
    // This would fetch all analysis results for a given file
    const asset = this.assets.get(fileid);
    if (!asset) return { error: "Asset not found" };
    
    // Get speaker segments if available
    let speakerSegments = [];
    const diarization = this.speakerDiarization.get(fileid);
    if (diarization) {
      speakerSegments = this.speakerSegments.get(diarization.id) || [];
    }
    
    // Get forbidden phrase details if available
    let forbiddenPhraseDetails = [];
    const forbiddenPhrases = this.forbiddenPhrases.get(fileid);
    if (forbiddenPhrases) {
      forbiddenPhraseDetails = this.forbiddenPhrasesDetails.get(forbiddenPhrases.id) || [];
    }
    
    return {
      asset,
      sentiment: this.sentiment.get(fileid),
      language: this.language.get(fileid),
      summarization: this.summarization.get(fileid),
      forbiddenPhrases,
      forbiddenPhraseDetails,
      topicModeling: this.topicModeling.get(fileid),
      speakerDiarization: diarization,
      speakerSegments
    };
  }

  async transcribeAndAnalyze(filename: string): Promise<any> {
    // Mock implementation (would be replaced with actual Deepgram API calls)
    const fileid = nanoid();
    
    // Create asset record
    const fileIndex = this.sourceFiles.findIndex(f => f.name === filename);
    if (fileIndex === -1) throw new Error("File not found");
    
    const file = this.sourceFiles[fileIndex];
    
    const asset: InsertRdtAsset = {
      fileid,
      filename: file.name,
      sourcePath: `shahulin/${file.name}`,
      destinationPath: `shahulout/${file.name}`,
      fileSize: file.size,
      status: 'completed',
      transcription: "This is a mock transcription for " + file.name,
      transcriptionJson: { results: { channels: [{ alternatives: [{ transcript: "This is a mock transcription for " + file.name }] }] } }
    };
    
    await this.createAsset(asset);
    
    // Create mock analysis results
    await this.saveSentimentAnalysis({
      fileid,
      overallSentiment: ['positive', 'neutral', 'negative'][Math.floor(Math.random() * 3)],
      confidenceScore: Math.floor(Math.random() * 100),
      sentimentBySegment: [{ text: "Sample segment", sentiment: "positive", confidence: 0.85 }]
    });
    
    await this.saveLanguageDetection({
      fileid,
      language: 'English',
      confidence: 98
    });
    
    await this.saveSummarization({
      fileid,
      summary: "This is a mock summary for " + file.name,
      summaryType: "short"
    });
    
    await this.saveForbiddenPhrases({
      fileid,
      riskScore: Math.floor(Math.random() * 100),
      riskLevel: ['low', 'medium', 'high'][Math.floor(Math.random() * 3)],
      categoriesDetected: { financial_promises: 2, misleading_claims: 1 }
    });
    
    await this.saveTopicModeling({
      fileid,
      topicsDetected: [
        { topic_id: 0, keywords: ["billing", "issue", "payment"], score: 0.8 },
        { topic_id: 1, keywords: ["technical", "support", "problem"], score: 0.6 }
      ]
    });
    
    await this.saveSpeakerDiarization({
      fileid,
      speakerCount: 2,
      speakerMetrics: {
        speakerTalkTime: { 0: 120, 1: 180 },
        speakerWordCount: { 0: 200, 1: 300 }
      }
    });
    
    // Move the file to processed
    await this.moveFileToProcessed(filename);
    
    return { fileid };
  }

  async saveSentimentAnalysis(data: InsertRdtSentiment): Promise<RdtSentiment> {
    const id = this.currentSentimentId++;
    const sentiment: RdtSentiment = {
      ...data,
      id,
      created_dt: new Date(),
      created_by: 1,
      status: 'completed'
    };
    this.sentiment.set(data.fileid, sentiment);
    return sentiment;
  }

  async saveLanguageDetection(data: InsertRdtLanguage): Promise<RdtLanguage> {
    const id = this.currentLanguageId++;
    const language: RdtLanguage = {
      ...data,
      id,
      created_dt: new Date(),
      created_by: 1,
      status: 'completed'
    };
    this.language.set(data.fileid, language);
    return language;
  }

  async saveSummarization(data: InsertRdtSummarization): Promise<RdtSummarization> {
    const id = this.currentSummarizationId++;
    const summarization: RdtSummarization = {
      ...data,
      id,
      created_dt: new Date(),
      created_by: 1,
      status: 'completed'
    };
    this.summarization.set(data.fileid, summarization);
    return summarization;
  }

  async saveForbiddenPhrases(data: InsertRdtForbiddenPhrases, details?: InsertRdtForbiddenPhrasesDetails[]): Promise<RdtForbiddenPhrases> {
    const id = this.currentForbiddenPhrasesId++;
    const forbiddenPhrases: RdtForbiddenPhrases = {
      ...data,
      id,
      created_dt: new Date(),
      created_by: 1,
      status: 'completed'
    };
    this.forbiddenPhrases.set(data.fileid, forbiddenPhrases);
    
    if (details && details.length > 0) {
      const detailsWithIds = details.map(detail => ({
        ...detail,
        id: this.currentForbiddenPhrasesDetailsId++,
        forbiddenPhraseId: id,
        created_dt: new Date()
      }));
      this.forbiddenPhrasesDetails.set(id, detailsWithIds);
    }
    
    return forbiddenPhrases;
  }

  async saveTopicModeling(data: InsertRdtTopicModeling): Promise<RdtTopicModeling> {
    const id = this.currentTopicModelingId++;
    const topicModeling: RdtTopicModeling = {
      ...data,
      id,
      created_dt: new Date(),
      created_by: 1,
      status: 'completed'
    };
    this.topicModeling.set(data.fileid, topicModeling);
    return topicModeling;
  }

  async saveSpeakerDiarization(data: InsertRdtSpeakerDiarization, segments?: InsertRdtSpeakerSegments[]): Promise<RdtSpeakerDiarization> {
    const id = this.currentSpeakerDiarizationId++;
    const speakerDiarization: RdtSpeakerDiarization = {
      ...data,
      id,
      created_dt: new Date(),
      created_by: 1,
      status: 'completed'
    };
    this.speakerDiarization.set(data.fileid, speakerDiarization);
    
    if (segments && segments.length > 0) {
      const segmentsWithIds = segments.map(segment => ({
        ...segment,
        id: this.currentSpeakerSegmentsId++,
        diarizationId: id,
        created_dt: new Date()
      }));
      this.speakerSegments.set(id, segmentsWithIds);
    }
    
    return speakerDiarization;
  }

  async getSentimentAnalysisByFileid(fileid: string): Promise<RdtSentiment | undefined> {
    return this.sentiment.get(fileid);
  }

  async getLanguageDetectionByFileid(fileid: string): Promise<RdtLanguage | undefined> {
    return this.language.get(fileid);
  }

  async getSummarizationByFileid(fileid: string): Promise<RdtSummarization | undefined> {
    return this.summarization.get(fileid);
  }

  async getForbiddenPhrasesByFileid(fileid: string): Promise<{phrases: RdtForbiddenPhrases, details: RdtForbiddenPhrasesDetails[]} | undefined> {
    const phrases = this.forbiddenPhrases.get(fileid);
    if (!phrases) return undefined;
    
    const details = this.forbiddenPhrasesDetails.get(phrases.id) || [];
    return { phrases, details };
  }

  async getTopicModelingByFileid(fileid: string): Promise<RdtTopicModeling | undefined> {
    return this.topicModeling.get(fileid);
  }

  async getSpeakerDiarizationByFileid(fileid: string): Promise<{diarization: RdtSpeakerDiarization, segments: RdtSpeakerSegments[]} | undefined> {
    const diarization = this.speakerDiarization.get(fileid);
    if (!diarization) return undefined;
    
    const segments = this.speakerSegments.get(diarization.id) || [];
    return { diarization, segments };
  }
}

export const storage = new MemStorage();
