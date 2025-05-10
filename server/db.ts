import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import * as schema from '@shared/schema';

// Create a PostgreSQL connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Create a Drizzle instance using the connection pool
export const db = drizzle(pool, { schema });

// Push the schema to the database
export async function pushSchema() {
  try {
    console.log("Attempting to push schema to database...");
    
    // Log number of tables being pushed
    console.log(`Pushing ${Object.keys(schema).length} tables to database...`);
    
    // Call the migrate function
    await pool.query(`
      BEGIN;
      ${(await import('fs')).readFileSync('./migrations/0000_salty_trish_tilby.sql', 'utf8')}
      COMMIT;
    `);
    
    console.log("Schema pushed successfully!");
    return true;
  } catch (error) {
    console.error("Error pushing schema:", error);
    return false;
  }
}