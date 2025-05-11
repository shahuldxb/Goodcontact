import { Pool } from 'pg';

// Create a PostgreSQL Pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

/**
 * Updates a transcription record directly in the database
 * 
 * @param fileid - The unique file ID
 * @param transcript - The transcript text
 * @param transcriptionJson - The full JSON response from the transcription service
 * @param language - The detected language
 * @param processingDuration - The processing duration in seconds
 */
export async function updateTranscriptionRecord(
  fileid: string, 
  transcript: string, 
  transcriptionJson: any,
  language: string,
  processingDuration: number
): Promise<boolean> {
  try {
    // Get a client from the pool
    const client = await pool.connect();
    
    try {
      // Serialize the JSON if it's an object
      let jsonString = '{}';
      if (transcriptionJson) {
        try {
          jsonString = typeof transcriptionJson === 'string' 
            ? transcriptionJson 
            : JSON.stringify(transcriptionJson);
        } catch (e) {
          console.error('Error converting transcription JSON to string', e);
        }
      }
      
      // Update the record
      const query = `
        UPDATE rdt_assets
        SET 
          status = $1,
          processed_date = $2,
          transcription = $3,
          transcription_json = $4,
          language_detected = $5,
          processing_duration = $6
        WHERE fileid = $7
      `;
      
      const values = [
        'completed', 
        new Date(), 
        transcript, 
        jsonString, 
        language || 'English', 
        processingDuration,
        fileid
      ];
      
      const result = await client.query(query, values);
      
      // Check if any rows were affected
      return result.rowCount !== null && result.rowCount > 0;
    } finally {
      // Release the client back to the pool
      client.release();
    }
  } catch (error) {
    console.error('Error updating transcription record:', error);
    return false;
  }
}

/**
 * Checks if a record exists in the database
 * 
 * @param fileid - The unique file ID
 */
export async function checkRecordExists(fileid: string): Promise<boolean> {
  try {
    const client = await pool.connect();
    try {
      const query = 'SELECT COUNT(*) FROM rdt_assets WHERE fileid = $1';
      const result = await client.query(query, [fileid]);
      return result.rows.length > 0 && parseInt(result.rows[0].count) > 0;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('Error checking if record exists:', error);
    return false;
  }
}

/**
 * Creates a new asset record in the database
 * 
 * @param fileid - The unique file ID
 * @param filename - The filename
 * @param sourcePath - The source path
 * @param fileSize - The file size in bytes
 */
export async function createAssetRecord(
  fileid: string,
  filename: string,
  sourcePath: string,
  fileSize: number
): Promise<boolean> {
  try {
    const client = await pool.connect();
    try {
      const query = `
        INSERT INTO rdt_assets (
          fileid, filename, source_path, file_size, status, created_by
        ) VALUES (
          $1, $2, $3, $4, $5, $6
        )
      `;
      
      const values = [
        fileid,
        filename,
        sourcePath,
        fileSize,
        'pending',
        1
      ];
      
      const result = await client.query(query, values);
      return result.rowCount !== null && result.rowCount > 0;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('Error creating asset record:', error);
    return false;
  }
}

/**
 * Retrieves an asset record from the database
 * 
 * @param fileid - The unique file ID
 */
export async function getAssetRecord(fileid: string): Promise<any> {
  try {
    const client = await pool.connect();
    try {
      const query = 'SELECT * FROM rdt_assets WHERE fileid = $1';
      const result = await client.query(query, [fileid]);
      return result.rows.length > 0 ? result.rows[0] : null;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('Error retrieving asset record:', error);
    return null;
  }
}