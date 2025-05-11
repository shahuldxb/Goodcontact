import { Pool } from 'pg';

// Create a PostgreSQL Pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// Connect to the database
export async function sqlConnect() {
  try {
    const client = await pool.connect();
    console.log('Connected to PostgreSQL');
    client.release();
    return pool;
  } catch (err) {
    console.error('PostgreSQL Connection Error:', err);
    throw err;
  }
}

// Execute query with parameters
export async function executeQuery(query: string, params: any[] = []) {
  try {
    const client = await pool.connect();
    try {
      const result = await client.query(query, params);
      return result.rows;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('PostgreSQL Query Error:', err);
    throw err;
  }
}

// Insert record and return the inserted ID
export async function insertRecord(tableName: string, data: Record<string, any>) {
  try {
    const fields = Object.keys(data);
    const placeholders = fields.map((_, i) => `$${i + 1}`).join(', ');
    const values = Object.values(data);
    
    const query = `
      INSERT INTO ${tableName} (${fields.join(', ')})
      VALUES (${placeholders})
      RETURNING id;
    `;
    
    const client = await pool.connect();
    try {
      const result = await client.query(query, values);
      return result.rows[0]?.id;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error(`Error inserting record into ${tableName}:`, err);
    throw err;
  }
}

// Update record by field name and value
export async function updateRecordByField(tableName: string, fieldName: string, fieldValue: any, data: Record<string, any>) {
  try {
    const updates = Object.keys(data).map((key, i) => `${key} = $${i + 1}`).join(', ');
    const values = [...Object.values(data), fieldValue];
    
    const query = `
      UPDATE ${tableName}
      SET ${updates}
      WHERE ${fieldName} = $${Object.values(data).length + 1};
    `;
    
    const client = await pool.connect();
    try {
      await client.query(query, values);
      return true;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error(`Error updating record in ${tableName}:`, err);
    throw err;
  }
}

// Get record by field name and value
export async function getRecordsByField(tableName: string, fieldName: string, fieldValue: any) {
  try {
    const query = `SELECT * FROM ${tableName} WHERE ${fieldName} = $1;`;
    
    const client = await pool.connect();
    try {
      const result = await client.query(query, [fieldValue]);
      return result.rows;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error(`Error getting records from ${tableName} by ${fieldName}:`, err);
    throw err;
  }
}