import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { sqlConnect } from './services/azure-sql';

// Get the directory name in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export async function createAzureTables() {
  try {
    console.log('Starting Azure SQL table creation...');
    
    // Read SQL script
    const sqlScript = readFileSync(join(__dirname, 'create-tables.sql'), 'utf8');
    
    // Connect to Azure SQL
    const pool = await sqlConnect();
    
    // Execute script
    const result = await pool.request().batch(sqlScript);
    
    console.log('Azure SQL tables created successfully');
    return true;
  } catch (error) {
    console.error('Error creating Azure SQL tables:', error);
    return false;
  }
}