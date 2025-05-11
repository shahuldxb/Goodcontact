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

export async function sqlConnect() {
  try {
    if (!pool) {
      pool = await new sql.ConnectionPool(sqlConfig).connect();
      console.log('Connected to Azure SQL Server');
    }
    return pool;
  } catch (err) {
    console.error('SQL Connection Error:', err);
    throw err;
  }
}

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
    console.error('SQL Query Error:', err);
    throw err;
  }
}

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

// Helper functions for common SQL operations

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

export async function getRecordsByField(tableName: string, fieldName: string, fieldValue: any) {
  try {
    const query = `SELECT * FROM ${tableName} WHERE ${fieldName} = @param0;`;
    return await executeQuery(query, [fieldValue]);
  } catch (err) {
    console.error(`Error getting records from ${tableName} by ${fieldName}:`, err);
    throw err;
  }
}

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
