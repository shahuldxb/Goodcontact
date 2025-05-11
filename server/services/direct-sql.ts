import sql from 'mssql';
import { azureConfig } from '@shared/schema';

// SQL Server configuration
const sqlConfig = {
  server: process.env.AZURE_SQL_SERVER || azureConfig.sqlServer,
  port: Number(process.env.AZURE_SQL_PORT) || azureConfig.sqlPort,
  database: process.env.AZURE_SQL_DATABASE || azureConfig.sqlDatabase,
  user: process.env.AZURE_SQL_USER || azureConfig.sqlUser,
  password: process.env.AZURE_SQL_PASSWORD || azureConfig.sqlPassword,
  options: {
    encrypt: true,
    trustServerCertificate: false,
    enableArithAbort: true
  }
};

// Create a pool of connections
let pool: sql.ConnectionPool;

// Get or create a connection pool
async function getPool(): Promise<sql.ConnectionPool> {
  if (!pool) {
    try {
      pool = await new sql.ConnectionPool(sqlConfig).connect();
      console.log('Connected to SQL Server directly');
    } catch (err) {
      console.error('Direct SQL Connection Error:', err);
      throw err;
    }
  }
  return pool;
}

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
    // Get the connection pool
    const pool = await getPool();
    
    // Prepare the request
    const request = pool.request();
    
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
    
    // Set the parameters with correct SQL types
    request.input('status', sql.VarChar(50), 'completed');
    request.input('processedDate', sql.DateTime, new Date());
    request.input('transcript', sql.NVarChar(sql.MAX), transcript);
    request.input('jsonData', sql.NVarChar(sql.MAX), jsonString);
    request.input('language', sql.VarChar(50), language || 'English');
    request.input('processingDuration', sql.Int, processingDuration);
    request.input('fileId', sql.VarChar(100), fileid);
    
    // Update the record
    const query = `
      UPDATE rdt_assets
      SET 
        status = @status,
        processed_date = @processedDate,
        transcription = @transcript,
        transcription_json = @jsonData,
        language_detected = @language,
        processing_duration = @processingDuration
      WHERE fileid = @fileId
    `;
    
    const result = await request.query(query);
    
    // Check if any rows were affected
    console.log('Successfully updated asset record in SQL Server database for', fileid);
    return result.rowsAffected[0] > 0;
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
    // Get the connection pool
    const pool = await getPool();
    
    // Prepare the request
    const request = pool.request();
    request.input('fileId', sql.VarChar(100), fileid);
    
    // Execute the query
    const query = 'SELECT COUNT(*) AS recordCount FROM rdt_assets WHERE fileid = @fileId';
    const result = await request.query(query);
    
    // Check if the record exists
    return result.recordset.length > 0 && result.recordset[0].recordCount > 0;
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
    // Get the connection pool
    const pool = await getPool();
    
    // Prepare the request
    const request = pool.request();
    
    // Set the parameters with correct SQL types
    request.input('fileId', sql.VarChar(100), fileid);
    request.input('filename', sql.VarChar(255), filename);
    request.input('sourcePath', sql.VarChar(255), sourcePath);
    request.input('fileSize', sql.Int, fileSize);
    request.input('status', sql.VarChar(50), 'pending');
    request.input('createdBy', sql.Int, 1);
    
    // Insert the record
    const query = `
      INSERT INTO rdt_assets (
        fileid, filename, source_path, file_size, status, created_by, upload_date, created_dt
      ) VALUES (
        @fileId, @filename, @sourcePath, @fileSize, @status, @createdBy, GETDATE(), GETDATE()
      )
    `;
    
    const result = await request.query(query);
    
    // Check if any rows were affected
    console.log('Successfully created asset record in SQL Server database for', fileid);
    return result.rowsAffected[0] > 0;
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
    // Get the connection pool
    const pool = await getPool();
    
    // Prepare the request
    const request = pool.request();
    request.input('fileId', sql.VarChar(100), fileid);
    
    // Execute the query
    const query = 'SELECT * FROM rdt_assets WHERE fileid = @fileId';
    const result = await request.query(query);
    
    // Return the record if it exists
    return result.recordset.length > 0 ? result.recordset[0] : null;
  } catch (error) {
    console.error('Error retrieving asset record:', error);
    return null;
  }
}