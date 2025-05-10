-- Master table for storing file information and transcription data
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_assets')
BEGIN
    CREATE TABLE rdt_assets (
        id INT IDENTITY(1,1) PRIMARY KEY,
        fileid NVARCHAR(255) NOT NULL UNIQUE,
        filename NVARCHAR(255) NOT NULL,
        source_path NVARCHAR(255) NOT NULL,
        destination_path NVARCHAR(255),
        file_size INT NOT NULL,
        upload_date DATETIME DEFAULT GETDATE() NOT NULL,
        processed_date DATETIME,
        transcription NVARCHAR(MAX),
        transcription_json NVARCHAR(MAX),
        status NVARCHAR(50) DEFAULT 'pending' NOT NULL,
        language_detected NVARCHAR(100),
        error_message NVARCHAR(MAX),
        processing_duration INT,
        created_dt DATETIME DEFAULT GETDATE() NOT NULL,
        created_by INT DEFAULT 1 NOT NULL
    );
    PRINT 'Created table: rdt_assets';
END
ELSE
    PRINT 'Table rdt_assets already exists';

-- Sentiment Analysis results
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_sentiment')
BEGIN
    CREATE TABLE rdt_sentiment (
        id INT IDENTITY(1,1) PRIMARY KEY,
        fileid NVARCHAR(255) NOT NULL,
        overall_sentiment NVARCHAR(50) NOT NULL,
        confidence_score INT,
        sentiment_by_segment NVARCHAR(MAX),
        created_dt DATETIME DEFAULT GETDATE() NOT NULL,
        created_by INT DEFAULT 1 NOT NULL,
        status NVARCHAR(50) DEFAULT 'completed' NOT NULL
    );
    PRINT 'Created table: rdt_sentiment';
END
ELSE
    PRINT 'Table rdt_sentiment already exists';

-- Language Detection results
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_language')
BEGIN
    CREATE TABLE rdt_language (
        id INT IDENTITY(1,1) PRIMARY KEY,
        fileid NVARCHAR(255) NOT NULL,
        language NVARCHAR(100) NOT NULL,
        confidence INT,
        created_dt DATETIME DEFAULT GETDATE() NOT NULL,
        created_by INT DEFAULT 1 NOT NULL,
        status NVARCHAR(50) DEFAULT 'completed' NOT NULL
    );
    PRINT 'Created table: rdt_language';
END
ELSE
    PRINT 'Table rdt_language already exists';

-- Call Summarization results
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_summarization')
BEGIN
    CREATE TABLE rdt_summarization (
        id INT IDENTITY(1,1) PRIMARY KEY,
        fileid NVARCHAR(255) NOT NULL,
        summary NVARCHAR(MAX) NOT NULL,
        summary_type NVARCHAR(50) NOT NULL,
        created_dt DATETIME DEFAULT GETDATE() NOT NULL,
        created_by INT DEFAULT 1 NOT NULL,
        status NVARCHAR(50) DEFAULT 'completed' NOT NULL
    );
    PRINT 'Created table: rdt_summarization';
END
ELSE
    PRINT 'Table rdt_summarization already exists';

-- Forbidden Phrases results
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_forbidden_phrases')
BEGIN
    CREATE TABLE rdt_forbidden_phrases (
        id INT IDENTITY(1,1) PRIMARY KEY,
        fileid NVARCHAR(255) NOT NULL,
        risk_score INT,
        risk_level NVARCHAR(50),
        categories_detected NVARCHAR(MAX),
        created_dt DATETIME DEFAULT GETDATE() NOT NULL,
        created_by INT DEFAULT 1 NOT NULL,
        status NVARCHAR(50) DEFAULT 'completed' NOT NULL
    );
    PRINT 'Created table: rdt_forbidden_phrases';
END
ELSE
    PRINT 'Table rdt_forbidden_phrases already exists';

-- Forbidden Phrases Details (sub-table for one-to-many relationship)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_forbidden_phrases_details')
BEGIN
    CREATE TABLE rdt_forbidden_phrases_details (
        id INT IDENTITY(1,1) PRIMARY KEY,
        forbidden_phrase_id INT NOT NULL,
        category NVARCHAR(100) NOT NULL,
        phrase NVARCHAR(255) NOT NULL,
        start_time INT,
        end_time INT,
        confidence INT,
        snippet NVARCHAR(MAX),
        created_dt DATETIME DEFAULT GETDATE() NOT NULL
    );
    PRINT 'Created table: rdt_forbidden_phrases_details';
END
ELSE
    PRINT 'Table rdt_forbidden_phrases_details already exists';

-- Topic Modeling results
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_topic_modeling')
BEGIN
    CREATE TABLE rdt_topic_modeling (
        id INT IDENTITY(1,1) PRIMARY KEY,
        fileid NVARCHAR(255) NOT NULL,
        topics_detected NVARCHAR(MAX),
        created_dt DATETIME DEFAULT GETDATE() NOT NULL,
        created_by INT DEFAULT 1 NOT NULL,
        status NVARCHAR(50) DEFAULT 'completed' NOT NULL
    );
    PRINT 'Created table: rdt_topic_modeling';
END
ELSE
    PRINT 'Table rdt_topic_modeling already exists';

-- Speaker Diarization results
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_speaker_diarization')
BEGIN
    CREATE TABLE rdt_speaker_diarization (
        id INT IDENTITY(1,1) PRIMARY KEY,
        fileid NVARCHAR(255) NOT NULL,
        speaker_count INT NOT NULL,
        speaker_metrics NVARCHAR(MAX),
        created_dt DATETIME DEFAULT GETDATE() NOT NULL,
        created_by INT DEFAULT 1 NOT NULL,
        status NVARCHAR(50) DEFAULT 'completed' NOT NULL
    );
    PRINT 'Created table: rdt_speaker_diarization';
END
ELSE
    PRINT 'Table rdt_speaker_diarization already exists';

-- Speaker Diarization Segments (sub-table for one-to-many relationship)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_speaker_segments')
BEGIN
    CREATE TABLE rdt_speaker_segments (
        id INT IDENTITY(1,1) PRIMARY KEY,
        diarization_id INT NOT NULL,
        speaker_id INT NOT NULL,
        text NVARCHAR(MAX) NOT NULL,
        start_time INT,
        end_time INT,
        created_dt DATETIME DEFAULT GETDATE() NOT NULL
    );
    PRINT 'Created table: rdt_speaker_segments';
END
ELSE
    PRINT 'Table rdt_speaker_segments already exists';

-- Add foreign key constraints
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_forbidden_phrases_details') AND EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_forbidden_phrases')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_forbidden_phrases_details_forbidden_phrases')
    BEGIN
        ALTER TABLE rdt_forbidden_phrases_details
        ADD CONSTRAINT FK_forbidden_phrases_details_forbidden_phrases
        FOREIGN KEY (forbidden_phrase_id) REFERENCES rdt_forbidden_phrases(id);
        PRINT 'Added foreign key: FK_forbidden_phrases_details_forbidden_phrases';
    END
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_speaker_segments') AND EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_speaker_diarization')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_speaker_segments_speaker_diarization')
    BEGIN
        ALTER TABLE rdt_speaker_segments
        ADD CONSTRAINT FK_speaker_segments_speaker_diarization
        FOREIGN KEY (diarization_id) REFERENCES rdt_speaker_diarization(id);
        PRINT 'Added foreign key: FK_speaker_segments_speaker_diarization';
    END
END