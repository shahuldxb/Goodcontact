import { readFileSync } from 'fs';
import { join } from 'path';
import { sqlConnect } from './services/azure-sql';

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