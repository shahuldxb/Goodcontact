import { pgTable, text, serial, integer, boolean, timestamp, jsonb, varchar, uniqueIndex } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// Master table for storing file information and transcription data
export const rdtAssets = pgTable("rdt_assets", {
  id: serial("id").primaryKey(),
  fileid: text("fileid").notNull().unique(), // Unique ID for the file
  filename: text("filename").notNull(),
  sourcePath: text("source_path").notNull(),
  destinationPath: text("destination_path"),
  fileSize: integer("file_size").notNull(),
  uploadDate: timestamp("upload_date").defaultNow().notNull(),
  processedDate: timestamp("processed_date"),
  transcription: text("transcription"), // Raw transcription text
  transcriptionJson: jsonb("transcription_json"), // Full Deepgram response
  status: text("status").default("pending").notNull(), // pending, processing, completed, error
  languageDetected: text("language_detected"),
  errorMessage: text("error_message"),
  processingDuration: integer("processing_duration"), // in milliseconds
  created_dt: timestamp("created_dt").defaultNow().notNull(),
  created_by: integer("created_by").default(1).notNull(),
});

// Sentiment Analysis results
export const rdtSentiment = pgTable("rdt_sentiment", {
  id: serial("id").primaryKey(),
  fileid: text("fileid").notNull(), // References fileid in rdt_assets
  overallSentiment: text("overall_sentiment").notNull(), // positive, negative, neutral
  confidenceScore: integer("confidence_score"), // 0-100
  sentimentBySegment: jsonb("sentiment_by_segment"), // JSON array of segment-level sentiment
  created_dt: timestamp("created_dt").defaultNow().notNull(),
  created_by: integer("created_by").default(1).notNull(),
  status: text("status").default("completed").notNull(),
});

// Language Detection results
export const rdtLanguage = pgTable("rdt_language", {
  id: serial("id").primaryKey(),
  fileid: text("fileid").notNull(),
  language: text("language").notNull(),
  confidence: integer("confidence"), // 0-100
  created_dt: timestamp("created_dt").defaultNow().notNull(),
  created_by: integer("created_by").default(1).notNull(),
  status: text("status").default("completed").notNull(),
});

// Call Summarization results
export const rdtSummarization = pgTable("rdt_summarization", {
  id: serial("id").primaryKey(),
  fileid: text("fileid").notNull(),
  summary: text("summary").notNull(),
  summaryType: text("summary_type").notNull(), // short, long
  created_dt: timestamp("created_dt").defaultNow().notNull(),
  created_by: integer("created_by").default(1).notNull(),
  status: text("status").default("completed").notNull(),
});

// Forbidden Phrases results
export const rdtForbiddenPhrases = pgTable("rdt_forbidden_phrases", {
  id: serial("id").primaryKey(),
  fileid: text("fileid").notNull(),
  riskScore: integer("risk_score"), // 0-100
  riskLevel: text("risk_level"), // low, medium, high
  categoriesDetected: jsonb("categories_detected"), // JSON object of categories and risk scores
  created_dt: timestamp("created_dt").defaultNow().notNull(),
  created_by: integer("created_by").default(1).notNull(),
  status: text("status").default("completed").notNull(),
});

// Forbidden Phrases Details (sub-table for one-to-many relationship)
export const rdtForbiddenPhrasesDetails = pgTable("rdt_forbidden_phrases_details", {
  id: serial("id").primaryKey(),
  forbiddenPhraseId: integer("forbidden_phrase_id").notNull(), // References id in rdt_forbidden_phrases
  category: text("category").notNull(),
  phrase: text("phrase").notNull(),
  startTime: integer("start_time"), // in seconds
  endTime: integer("end_time"), // in seconds
  confidence: integer("confidence"), // 0-100
  snippet: text("snippet"), // Context around the phrase
  created_dt: timestamp("created_dt").defaultNow().notNull(),
});

// Topic Modeling results
export const rdtTopicModeling = pgTable("rdt_topic_modeling", {
  id: serial("id").primaryKey(),
  fileid: text("fileid").notNull(),
  topicsDetected: jsonb("topics_detected"), // JSON array of topics with weights
  created_dt: timestamp("created_dt").defaultNow().notNull(),
  created_by: integer("created_by").default(1).notNull(),
  status: text("status").default("completed").notNull(),
});

// Speaker Diarization results
export const rdtSpeakerDiarization = pgTable("rdt_speaker_diarization", {
  id: serial("id").primaryKey(),
  fileid: text("fileid").notNull(),
  speakerCount: integer("speaker_count").notNull(),
  speakerMetrics: jsonb("speaker_metrics"), // Talk time, word count, etc.
  created_dt: timestamp("created_dt").defaultNow().notNull(),
  created_by: integer("created_by").default(1).notNull(),
  status: text("status").default("completed").notNull(),
});

// Speaker Diarization Segments (sub-table for one-to-many relationship)
export const rdtSpeakerSegments = pgTable("rdt_speaker_segments", {
  id: serial("id").primaryKey(),
  diarizationId: integer("diarization_id").notNull(), // References id in rdt_speaker_diarization
  speakerId: integer("speaker_id").notNull(),
  text: text("text").notNull(),
  startTime: integer("start_time"), // in seconds
  endTime: integer("end_time"), // in seconds
  created_dt: timestamp("created_dt").defaultNow().notNull(),
});

// Insert Schemas
export const insertRdtAssetSchema = createInsertSchema(rdtAssets).omit({
  id: true,
  created_dt: true
});

export const insertRdtSentimentSchema = createInsertSchema(rdtSentiment).omit({
  id: true, 
  created_dt: true
});

export const insertRdtLanguageSchema = createInsertSchema(rdtLanguage).omit({
  id: true,
  created_dt: true
});

export const insertRdtSummarizationSchema = createInsertSchema(rdtSummarization).omit({
  id: true,
  created_dt: true
});

export const insertRdtForbiddenPhrasesSchema = createInsertSchema(rdtForbiddenPhrases).omit({
  id: true,
  created_dt: true
});

export const insertRdtForbiddenPhrasesDetailsSchema = createInsertSchema(rdtForbiddenPhrasesDetails).omit({
  id: true,
  created_dt: true
});

export const insertRdtTopicModelingSchema = createInsertSchema(rdtTopicModeling).omit({
  id: true,
  created_dt: true
});

export const insertRdtSpeakerDiarizationSchema = createInsertSchema(rdtSpeakerDiarization).omit({
  id: true,
  created_dt: true
});

export const insertRdtSpeakerSegmentsSchema = createInsertSchema(rdtSpeakerSegments).omit({
  id: true,
  created_dt: true
});

// Type Definitions
export type InsertRdtAsset = z.infer<typeof insertRdtAssetSchema>;
export type RdtAsset = typeof rdtAssets.$inferSelect;

export type InsertRdtSentiment = z.infer<typeof insertRdtSentimentSchema>;
export type RdtSentiment = typeof rdtSentiment.$inferSelect;

export type InsertRdtLanguage = z.infer<typeof insertRdtLanguageSchema>;
export type RdtLanguage = typeof rdtLanguage.$inferSelect;

export type InsertRdtSummarization = z.infer<typeof insertRdtSummarizationSchema>;
export type RdtSummarization = typeof rdtSummarization.$inferSelect;

export type InsertRdtForbiddenPhrases = z.infer<typeof insertRdtForbiddenPhrasesSchema>;
export type RdtForbiddenPhrases = typeof rdtForbiddenPhrases.$inferSelect;

export type InsertRdtForbiddenPhrasesDetails = z.infer<typeof insertRdtForbiddenPhrasesDetailsSchema>;
export type RdtForbiddenPhrasesDetails = typeof rdtForbiddenPhrasesDetails.$inferSelect;

export type InsertRdtTopicModeling = z.infer<typeof insertRdtTopicModelingSchema>;
export type RdtTopicModeling = typeof rdtTopicModeling.$inferSelect;

export type InsertRdtSpeakerDiarization = z.infer<typeof insertRdtSpeakerDiarizationSchema>;
export type RdtSpeakerDiarization = typeof rdtSpeakerDiarization.$inferSelect;

export type InsertRdtSpeakerSegments = z.infer<typeof insertRdtSpeakerSegmentsSchema>;
export type RdtSpeakerSegments = typeof rdtSpeakerSegments.$inferSelect;

// Configuration for Azure services
export const azureConfig = {
  storageAccountUrl: "https://infolder.blob.core.windows.net",
  storageAccountKey: "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==",
  sourceContainerName: "shahulin",
  destinationContainerName: "shahulout",
  sqlServer: "callcenter1.database.windows.net",
  sqlPort: 1433,
  sqlDatabase: "call",
  sqlUser: "shahul",
  sqlPassword: "apple123!@#",
  deepgramKey: "d6290865c35bddd50928c5d26983769682fca987"
};
