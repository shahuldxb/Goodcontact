import sql from 'mssql';

// SQL Server configuration
const sqlConfig = {
  server: process.env.SQLSERVER || 'localhost',
  database: process.env.SQLDATABASE || 'shahulin',
  user: process.env.SQLUSER || 'SA',
  password: process.env.SQLPASSWORD || 'yourStrong(!)Password',
  options: {
    encrypt: true,
    trustServerCertificate: true,
    enableArithAbort: true
  }
};

async function checkTables() {
  try {
    // Connect to SQL Server
    const pool = await new sql.ConnectionPool(sqlConfig).connect();
    console.log('Connected to SQL Server');
    
    // Check tables
    const tableResult = await pool.request().query(`
      SELECT name FROM sys.tables WHERE name LIKE 'rdt_%'
    `);
    
    console.log('Tables in SQL Server:');
    tableResult.recordset.forEach(table => {
      console.log(`- ${table.name}`);
    });
    
    // Check if rdt_paragraphs and rdt_sentences have any data
    if (tableResult.recordset.some(t => t.name === 'rdt_paragraphs')) {
      const paragraphsCount = await pool.request().query(`
        SELECT COUNT(*) as count FROM rdt_paragraphs
      `);
      console.log(`\nParagraphs count: ${paragraphsCount.recordset[0].count}`);
      
      // Get sample paragraphs
      if (paragraphsCount.recordset[0].count > 0) {
        const sampleParagraphs = await pool.request().query(`
          SELECT TOP 3 * FROM rdt_paragraphs
        `);
        console.log('\nSample paragraphs:');
        console.log(JSON.stringify(sampleParagraphs.recordset, null, 2));
      }
    } else {
      console.log('\nrdt_paragraphs table does not exist');
    }
    
    if (tableResult.recordset.some(t => t.name === 'rdt_sentences')) {
      const sentencesCount = await pool.request().query(`
        SELECT COUNT(*) as count FROM rdt_sentences
      `);
      console.log(`\nSentences count: ${sentencesCount.recordset[0].count}`);
      
      // Get sample sentences
      if (sentencesCount.recordset[0].count > 0) {
        const sampleSentences = await pool.request().query(`
          SELECT TOP 3 * FROM rdt_sentences
        `);
        console.log('\nSample sentences:');
        console.log(JSON.stringify(sampleSentences.recordset, null, 2));
      }
    } else {
      console.log('\nrdt_sentences table does not exist');
    }
    
    // Check for stored procedures
    const proceduresResult = await pool.request().query(`
      SELECT name FROM sys.procedures WHERE name IN ('RDS_InsertParagraph', 'RDS_InsertSentence')
    `);
    
    console.log('\nStored procedures in SQL Server:');
    proceduresResult.recordset.forEach(proc => {
      console.log(`- ${proc.name}`);
    });
    
    // Close connection
    await pool.close();
    
  } catch (err) {
    console.error('Error checking SQL Server tables:', err);
  }
}

checkTables().catch(err => {
  console.error('Error running script:', err);
});