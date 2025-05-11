-- Transcription audio metadata table to store additional information about the audio file
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_audio_metadata')
BEGIN
    CREATE TABLE rdt_audio_metadata (
        id INT IDENTITY(1,1) PRIMARY KEY,
        fileid NVARCHAR(255) NOT NULL UNIQUE,
        request_id NVARCHAR(255),
        sha256 NVARCHAR(255),
        created_timestamp NVARCHAR(50),
        audio_duration FLOAT,
        confidence FLOAT,
        created_dt DATETIME DEFAULT GETDATE() NOT NULL,
        created_by INT DEFAULT 1 NOT NULL,
        status NVARCHAR(50) DEFAULT 'completed' NOT NULL
    );
    PRINT 'Created table: rdt_audio_metadata';
END
ELSE
    PRINT 'Table rdt_audio_metadata already exists';

-- Paragraphs table (one-to-many with the audio file)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_paragraphs')
BEGIN
    CREATE TABLE rdt_paragraphs (
        id INT IDENTITY(1,1) PRIMARY KEY,
        fileid NVARCHAR(255) NOT NULL,
        paragraph_idx INT NOT NULL,
        text NVARCHAR(MAX) NOT NULL,
        start_time FLOAT,
        end_time FLOAT,
        speaker NVARCHAR(50),
        num_words INT,
        created_dt DATETIME DEFAULT GETDATE() NOT NULL
    );
    PRINT 'Created table: rdt_paragraphs';
END
ELSE
    PRINT 'Table rdt_paragraphs already exists';

-- Sentences table (one-to-many with paragraphs)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_sentences')
BEGIN
    CREATE TABLE rdt_sentences (
        id INT IDENTITY(1,1) PRIMARY KEY,
        fileid NVARCHAR(255) NOT NULL,
        paragraph_id INT NOT NULL,
        sentence_idx NVARCHAR(50) NOT NULL,
        text NVARCHAR(MAX) NOT NULL,
        start_time FLOAT,
        end_time FLOAT,
        created_dt DATETIME DEFAULT GETDATE() NOT NULL
    );
    PRINT 'Created table: rdt_sentences';
END
ELSE
    PRINT 'Table rdt_sentences already exists';

-- Add foreign key constraints
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_paragraphs') AND EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_assets')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_paragraphs_assets')
    BEGIN
        ALTER TABLE rdt_paragraphs
        ADD CONSTRAINT FK_paragraphs_assets
        FOREIGN KEY (fileid) REFERENCES rdt_assets(fileid);
        PRINT 'Added foreign key: FK_paragraphs_assets';
    END
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_sentences') AND EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_paragraphs')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_sentences_paragraphs')
    BEGIN
        ALTER TABLE rdt_sentences
        ADD CONSTRAINT FK_sentences_paragraphs
        FOREIGN KEY (paragraph_id) REFERENCES rdt_paragraphs(id);
        PRINT 'Added foreign key: FK_sentences_paragraphs';
    END
END

-- Create stored procedure for inserting audio metadata
IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = 'RDS_InsertAudioMetadata')
BEGIN
    EXEC('
    CREATE PROCEDURE RDS_InsertAudioMetadata
        @fileid NVARCHAR(255),
        @request_id NVARCHAR(255),
        @sha256 NVARCHAR(255),
        @created_timestamp NVARCHAR(50),
        @audio_duration FLOAT,
        @confidence FLOAT,
        @status NVARCHAR(50) = ''completed''
    AS
    BEGIN
        -- Check if the record already exists
        IF EXISTS (SELECT 1 FROM rdt_audio_metadata WHERE fileid = @fileid)
        BEGIN
            -- Update the existing record
            UPDATE rdt_audio_metadata
            SET 
                request_id = @request_id,
                sha256 = @sha256,
                created_timestamp = @created_timestamp,
                audio_duration = @audio_duration,
                confidence = @confidence,
                status = @status,
                created_dt = GETDATE()
            WHERE fileid = @fileid;
        END
        ELSE
        BEGIN
            -- Insert a new record
            INSERT INTO rdt_audio_metadata (
                fileid, 
                request_id, 
                sha256, 
                created_timestamp, 
                audio_duration, 
                confidence, 
                status
            )
            VALUES (
                @fileid, 
                @request_id, 
                @sha256, 
                @created_timestamp, 
                @audio_duration, 
                @confidence, 
                @status
            );
        END
    END
    ');
    PRINT 'Created stored procedure: RDS_InsertAudioMetadata';
END
ELSE
    PRINT 'Stored procedure RDS_InsertAudioMetadata already exists';

-- Create stored procedure for inserting paragraphs
IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = 'RDS_InsertParagraph')
BEGIN
    EXEC('
    CREATE PROCEDURE RDS_InsertParagraph
        @fileid NVARCHAR(255),
        @paragraph_idx INT,
        @text NVARCHAR(MAX),
        @start_time FLOAT,
        @end_time FLOAT,
        @speaker NVARCHAR(50),
        @num_words INT,
        @paragraph_id INT OUTPUT
    AS
    BEGIN
        -- Delete existing paragraphs for this fileid and paragraph_idx
        DELETE FROM rdt_sentences 
        WHERE paragraph_id IN (
            SELECT id FROM rdt_paragraphs 
            WHERE fileid = @fileid AND paragraph_idx = @paragraph_idx
        );
        
        DELETE FROM rdt_paragraphs 
        WHERE fileid = @fileid AND paragraph_idx = @paragraph_idx;
        
        -- Insert new paragraph
        INSERT INTO rdt_paragraphs (
            fileid,
            paragraph_idx,
            text,
            start_time,
            end_time,
            speaker,
            num_words
        )
        VALUES (
            @fileid,
            @paragraph_idx,
            @text,
            @start_time,
            @end_time,
            @speaker,
            @num_words
        );
        
        -- Return the new paragraph ID
        SET @paragraph_id = SCOPE_IDENTITY();
    END
    ');
    PRINT 'Created stored procedure: RDS_InsertParagraph';
END
ELSE
    PRINT 'Stored procedure RDS_InsertParagraph already exists';

-- Create stored procedure for inserting sentences
IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = 'RDS_InsertSentence')
BEGIN
    EXEC('
    CREATE PROCEDURE RDS_InsertSentence
        @fileid NVARCHAR(255),
        @paragraph_id INT,
        @sentence_idx NVARCHAR(50),
        @text NVARCHAR(MAX),
        @start_time FLOAT,
        @end_time FLOAT
    AS
    BEGIN
        -- Delete existing sentence for this paragraph_id and sentence_idx (if exists)
        DELETE FROM rdt_sentences 
        WHERE paragraph_id = @paragraph_id AND sentence_idx = @sentence_idx;
        
        -- Insert new sentence
        INSERT INTO rdt_sentences (
            fileid,
            paragraph_id,
            sentence_idx,
            text,
            start_time,
            end_time
        )
        VALUES (
            @fileid,
            @paragraph_id,
            @sentence_idx,
            @text,
            @start_time,
            @end_time
        );
    END
    ');
    PRINT 'Created stored procedure: RDS_InsertSentence';
END
ELSE
    PRINT 'Stored procedure RDS_InsertSentence already exists';