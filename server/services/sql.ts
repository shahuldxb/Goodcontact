/**
 * Unified SQL Service Module
 * 
 * This module provides a unified SQL interface that exclusively uses Azure SQL Server.
 * It replaces all previous PostgreSQL-based connections with Azure SQL Server.
 * 
 * Important: This module is the ONLY database connection module that should be used
 * throughout the application. All database operations must use this service.
 */

import sql from 'mssql';
import { azureConfig } from '@shared/schema';

// SQL configuration
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

/**
 * Connect to the Azure SQL database
 * Returns a connection pool that can be used for queries
 */
export async function sqlConnect() {
  try {
    if (!pool) {
      pool = await new sql.ConnectionPool(sqlConfig).connect();
      console.log('Connected to Azure SQL Server');
    }
    return pool;
  } catch (err) {
    console.error('Azure SQL Connection Error:', err);
    throw err;
  }
}

/**
 * Execute a SQL query with parameters
 * @param query The SQL query to execute
 * @param params Array of parameter values
 * @returns The query result set
 */
export async function executeQuery(query: string, params: any[] = []) {
  try {
    const pool = await sqlConnect();
    const request = pool.request();
    
    // Add parameters to the request with proper SQL types
    params.forEach((param, index) => {
      if (param === null || param === undefined) {
        request.input(`param${index}`, sql.VarChar, null);
      } 
      else if (typeof param === 'string') {
        // Sanitize string values - limit length to prevent overflow
        const safeValue = param.toString().substring(0, 4000);
        request.input(`param${index}`, sql.NVarChar(sql.MAX), safeValue);
      }
      else if (typeof param === 'number') {
        if (Number.isInteger(param)) {
          request.input(`param${index}`, sql.Int, param);
        } else {
          request.input(`param${index}`, sql.Float, param);
        }
      }
      else if (param instanceof Date) {
        request.input(`param${index}`, sql.DateTime, param);
      }
      else if (typeof param === 'boolean') {
        request.input(`param${index}`, sql.Bit, param);
      }
      else if (typeof param === 'object') {
        try {
          // For objects, convert to JSON string
          const jsonStr = JSON.stringify(param).substring(0, 4000);
          request.input(`param${index}`, sql.NVarChar(sql.MAX), jsonStr);
        } catch (e) {
          console.warn(`Failed to stringify object param${index}:`, e);
          request.input(`param${index}`, sql.NVarChar(sql.MAX), '{}');
        }
      }
      else {
        // Default fallback - convert to string
        request.input(`param${index}`, sql.NVarChar(1000), String(param).substring(0, 1000));
      }
    });
    
    const result = await request.query(query);
    return result.recordset;
  } catch (err) {
    console.error('Azure SQL Query Error:', err);
    throw err;
  }
}

/**
 * Execute a stored procedure
 * @param procedureName The name of the stored procedure
 * @param params Object with parameter names and values
 * @returns The procedure result set
 */
export async function executeStoredProcedure(procedureName: string, params: Record<string, any> = {}) {
  try {
    const pool = await sqlConnect();
    const request = pool.request();
    
    // Add parameters to the request
    Object.entries(params).forEach(([key, value]) => {
      request.input(key, value);
    });
    
    const result = await request.execute(procedureName);
    return result.recordset;
  } catch (err) {
    console.error(`Error executing stored procedure ${procedureName}:`, err);
    throw err;
  }
}

/**
 * Insert a record into a table
 * @param tableName The name of the table
 * @param data Object with column names and values
 * @returns The ID of the inserted record
 */
export async function insertRecord(tableName: string, data: Record<string, any>) {
  try {
    const fields = Object.keys(data);
    const values = Object.values(data);
    const paramPlaceholders = fields.map((_, i) => `@param${i}`).join(', ');
    
    const query = `
      INSERT INTO ${tableName} (${fields.join(', ')})
      VALUES (${paramPlaceholders});
      SELECT SCOPE_IDENTITY() AS id;
    `;
    
    const result = await executeQuery(query, values);
    return result?.[0]?.id;
  } catch (err) {
    console.error(`Error inserting record into ${tableName}:`, err);
    throw err;
  }
}

/**
 * Update a record by ID
 * @param tableName The name of the table
 * @param id The ID of the record to update
 * @param data Object with column names and values to update
 * @returns True if successful
 */
export async function updateRecord(tableName: string, id: number, data: Record<string, any>) {
  try {
    const updates = Object.entries(data)
      .map(([key, _], i) => `${key} = @param${i}`)
      .join(', ');
    
    const values = [...Object.values(data), id];
    
    const query = `
      UPDATE ${tableName}
      SET ${updates}
      WHERE id = @param${Object.values(data).length};
    `;
    
    await executeQuery(query, values);
    return true;
  } catch (err) {
    console.error(`Error updating record in ${tableName}:`, err);
    throw err;
  }
}

/**
 * Update a record by field name and value
 * @param tableName The name of the table
 * @param fieldName The field name to match in the WHERE clause
 * @param fieldValue The field value to match
 * @param data Object with column names and values to update
 * @returns True if successful
 */
export async function updateRecordByField(tableName: string, fieldName: string, fieldValue: any, data: Record<string, any>) {
  try {
    const updates = Object.entries(data)
      .map(([key, _], i) => `${key} = @param${i}`)
      .join(', ');
    
    const values = [...Object.values(data), fieldValue];
    
    const query = `
      UPDATE ${tableName}
      SET ${updates}
      WHERE ${fieldName} = @param${Object.values(data).length};
    `;
    
    await executeQuery(query, values);
    return true;
  } catch (err) {
    console.error(`Error updating record in ${tableName} by ${fieldName}:`, err);
    throw err;
  }
}

/**
 * Get a record by ID
 * @param tableName The name of the table
 * @param id The ID of the record
 * @returns The record or undefined
 */
export async function getRecordById(tableName: string, id: number) {
  try {
    const query = `SELECT * FROM ${tableName} WHERE id = @param0;`;
    const result = await executeQuery(query, [id]);
    return result?.[0];
  } catch (err) {
    console.error(`Error getting record from ${tableName}:`, err);
    throw err;
  }
}

/**
 * Get records by field value
 * @param tableName The name of the table
 * @param fieldName The field name to match
 * @param fieldValue The field value to match
 * @returns Array of matching records
 */
export async function getRecordsByField(tableName: string, fieldName: string, fieldValue: any) {
  try {
    const query = `SELECT * FROM ${tableName} WHERE ${fieldName} = @param0;`;
    return await executeQuery(query, [fieldValue]);
  } catch (err) {
    console.error(`Error getting records from ${tableName} by ${fieldName}:`, err);
    throw err;
  }
}

/**
 * Delete a record by ID
 * @param tableName The name of the table
 * @param id The ID of the record to delete
 * @returns True if successful
 */
export async function deleteRecord(tableName: string, id: number) {
  try {
    const query = `DELETE FROM ${tableName} WHERE id = @param0;`;
    await executeQuery(query, [id]);
    return true;
  } catch (err) {
    console.error(`Error deleting record from ${tableName}:`, err);
    throw err;
  }
}