CREATE TABLE "rdt_assets" (
	"id" serial PRIMARY KEY NOT NULL,
	"fileid" text NOT NULL,
	"filename" text NOT NULL,
	"source_path" text NOT NULL,
	"destination_path" text,
	"file_size" integer NOT NULL,
	"upload_date" timestamp DEFAULT now() NOT NULL,
	"processed_date" timestamp,
	"transcription" text,
	"transcription_json" jsonb,
	"status" text DEFAULT 'pending' NOT NULL,
	"language_detected" text,
	"error_message" text,
	"processing_duration" integer,
	"created_dt" timestamp DEFAULT now() NOT NULL,
	"created_by" integer DEFAULT 1 NOT NULL,
	CONSTRAINT "rdt_assets_fileid_unique" UNIQUE("fileid")
);
--> statement-breakpoint
CREATE TABLE "rdt_forbidden_phrases" (
	"id" serial PRIMARY KEY NOT NULL,
	"fileid" text NOT NULL,
	"risk_score" integer,
	"risk_level" text,
	"categories_detected" jsonb,
	"created_dt" timestamp DEFAULT now() NOT NULL,
	"created_by" integer DEFAULT 1 NOT NULL,
	"status" text DEFAULT 'completed' NOT NULL
);
--> statement-breakpoint
CREATE TABLE "rdt_forbidden_phrases_details" (
	"id" serial PRIMARY KEY NOT NULL,
	"forbidden_phrase_id" integer NOT NULL,
	"category" text NOT NULL,
	"phrase" text NOT NULL,
	"start_time" integer,
	"end_time" integer,
	"confidence" integer,
	"snippet" text,
	"created_dt" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "rdt_language" (
	"id" serial PRIMARY KEY NOT NULL,
	"fileid" text NOT NULL,
	"language" text NOT NULL,
	"confidence" integer,
	"created_dt" timestamp DEFAULT now() NOT NULL,
	"created_by" integer DEFAULT 1 NOT NULL,
	"status" text DEFAULT 'completed' NOT NULL
);
--> statement-breakpoint
CREATE TABLE "rdt_sentiment" (
	"id" serial PRIMARY KEY NOT NULL,
	"fileid" text NOT NULL,
	"overall_sentiment" text NOT NULL,
	"confidence_score" integer,
	"sentiment_by_segment" jsonb,
	"created_dt" timestamp DEFAULT now() NOT NULL,
	"created_by" integer DEFAULT 1 NOT NULL,
	"status" text DEFAULT 'completed' NOT NULL
);
--> statement-breakpoint
CREATE TABLE "rdt_speaker_diarization" (
	"id" serial PRIMARY KEY NOT NULL,
	"fileid" text NOT NULL,
	"speaker_count" integer NOT NULL,
	"speaker_metrics" jsonb,
	"created_dt" timestamp DEFAULT now() NOT NULL,
	"created_by" integer DEFAULT 1 NOT NULL,
	"status" text DEFAULT 'completed' NOT NULL
);
--> statement-breakpoint
CREATE TABLE "rdt_speaker_segments" (
	"id" serial PRIMARY KEY NOT NULL,
	"diarization_id" integer NOT NULL,
	"speaker_id" integer NOT NULL,
	"text" text NOT NULL,
	"start_time" integer,
	"end_time" integer,
	"created_dt" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "rdt_summarization" (
	"id" serial PRIMARY KEY NOT NULL,
	"fileid" text NOT NULL,
	"summary" text NOT NULL,
	"summary_type" text NOT NULL,
	"created_dt" timestamp DEFAULT now() NOT NULL,
	"created_by" integer DEFAULT 1 NOT NULL,
	"status" text DEFAULT 'completed' NOT NULL
);
--> statement-breakpoint
CREATE TABLE "rdt_topic_modeling" (
	"id" serial PRIMARY KEY NOT NULL,
	"fileid" text NOT NULL,
	"topics_detected" jsonb,
	"created_dt" timestamp DEFAULT now() NOT NULL,
	"created_by" integer DEFAULT 1 NOT NULL,
	"status" text DEFAULT 'completed' NOT NULL
);
