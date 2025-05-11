import { spawn } from 'child_process';
import fetch from 'node-fetch';
import { storage } from '../storage';
import path from 'path';
import fs from 'fs';

const PYTHON_SERVER_URL = 'http://localhost:5001';

// Initialize Python server
let pythonProcess: any = null;

export async function startPythonBackend() {
  if (pythonProcess) {
    console.log('Python backend is already running');
    return;
  }

  const pythonScript = path.join(process.cwd(), 'python_backend', 'app.py');
  
  if (!fs.existsSync(pythonScript)) {
    console.error(`Python script not found at ${pythonScript}`);
    return;
  }

  // Run Python script through the Python interpreter
  pythonProcess = spawn('python', [pythonScript]);

  pythonProcess.stdout.on('data', (data: Buffer) => {
    console.log(`Python stdout: ${data.toString()}`);
  });

  pythonProcess.stderr.on('data', (data: Buffer) => {
    console.error(`Python stderr: ${data.toString()}`);
  });

  pythonProcess.on('close', (code: number) => {
    console.log(`Python process exited with code ${code}`);
    pythonProcess = null;
  });

  // Wait for the server to start
  let maxAttempts = 10;
  let attempt = 0;
  
  while (attempt < maxAttempts) {
    try {
      const response = await fetch(`${PYTHON_SERVER_URL}/health`);
      if (response.ok) {
        console.log('Python backend is running');
        return;
      }
    } catch (e) {
      console.log(`Waiting for Python backend to start (attempt ${attempt + 1}/${maxAttempts})...`);
    }
    
    await new Promise(resolve => setTimeout(resolve, 1000));
    attempt++;
  }
  
  console.error('Failed to start Python backend');
}

export async function stopPythonBackend() {
  if (pythonProcess) {
    pythonProcess.kill();
    pythonProcess = null;
    console.log('Python backend stopped');
  }
}

export async function processFile(filename: string, fileid?: string) {
  try {
    console.log(`Processing file: ${filename} with ID: ${fileid}`);
    
    const response = await fetch(`${PYTHON_SERVER_URL}/process`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ filename, fileid })
    });
    
    if (!response.ok) {
      throw new Error(`Failed to process file: ${response.statusText}`);
    }
    
    const result = await response.json();
    console.log(`Processing result: ${JSON.stringify(result)}`);
    
    return result;
  } catch (e) {
    console.error(`Error processing file: ${e}`);
    throw e;
  }
}

export async function getSourceFiles() {
  try {
    const response = await fetch(`${PYTHON_SERVER_URL}/files/source`);
    
    if (!response.ok) {
      throw new Error(`Failed to get source files: ${response.statusText}`);
    }
    
    const result = await response.json();
    return result;
  } catch (e) {
    console.error(`Error getting source files: ${e}`);
    throw e;
  }
}

export async function getProcessedFiles() {
  try {
    const response = await fetch(`${PYTHON_SERVER_URL}/files/processed`);
    
    if (!response.ok) {
      throw new Error(`Failed to get processed files: ${response.statusText}`);
    }
    
    const result = await response.json();
    return result;
  } catch (e) {
    console.error(`Error getting processed files: ${e}`);
    throw e;
  }
}

export async function getAnalysisResults(fileid: string) {
  try {
    const response = await fetch(`${PYTHON_SERVER_URL}/analysis/${fileid}`);
    
    if (!response.ok) {
      throw new Error(`Failed to get analysis results: ${response.statusText}`);
    }
    
    const result = await response.json();
    return result;
  } catch (e) {
    console.error(`Error getting analysis results: ${e}`);
    // Fallback to Node.js implementation
    return await storage.getAnalysisResults(fileid);
  }
}

export async function getStats() {
  try {
    const response = await fetch(`${PYTHON_SERVER_URL}/stats`);
    
    if (!response.ok) {
      throw new Error(`Failed to get stats: ${response.statusText}`);
    }
    
    const result = await response.json();
    return result;
  } catch (e) {
    console.error(`Error getting stats: ${e}`);
    // Fallback to Node.js implementation
    return await storage.getStats();
  }
}

export async function getSentimentStats() {
  try {
    const response = await fetch(`${PYTHON_SERVER_URL}/stats/sentiment`);
    
    if (!response.ok) {
      throw new Error(`Failed to get sentiment stats: ${response.statusText}`);
    }
    
    const result = await response.json();
    return result;
  } catch (e) {
    console.error(`Error getting sentiment stats: ${e}`);
    // Fallback to Node.js implementation
    return await storage.getSentimentStats();
  }
}

export async function getTopicStats() {
  try {
    const response = await fetch(`${PYTHON_SERVER_URL}/stats/topics`);
    
    if (!response.ok) {
      throw new Error(`Failed to get topic stats: ${response.statusText}`);
    }
    
    const result = await response.json();
    return result;
  } catch (e) {
    console.error(`Error getting topic stats: ${e}`);
    // Fallback to Node.js implementation
    return await storage.getTopicStats();
  }
}

// Export a singleton instance
// Create a proxy middleware function for Express
export function createPythonProxyMiddleware(pythonEndpoint: string, method: 'GET' | 'POST' = 'GET') {
  return async (req: any, res: any) => {
    try {
      // Handle file uploads differently than regular POST requests
      const isFileUpload = req.is('multipart/form-data') || (req.headers['content-type'] && req.headers['content-type'].includes('multipart/form-data'));
      
      if (method === 'POST' && isFileUpload && req.files) {
        // For file uploads, we need to forward the file to Python backend
        console.log('Handling file upload to Python backend');
        
        // Extract the file from the request (assuming using multer or express-fileupload)
        const file = req.files.file;
        
        if (!file) {
          return res.status(400).json({ error: 'No file provided' });
        }
        
        // Create a FormData object to send the file
        const formData = new FormData();
        formData.append('file', file.buffer || file.data, {
          filename: file.originalname || file.name,
          contentType: file.mimetype
        });
        
        // Send the file to Python backend
        const response = await fetch(`${PYTHON_SERVER_URL}${pythonEndpoint}`, {
          method: 'POST',
          body: formData
        });
        
        if (!response.ok) {
          throw new Error(`Python server responded with status: ${response.status}`);
        }
        
        const data = await response.json();
        return res.json(data);
      }
      
      // For regular requests (non-file uploads)
      const url = `${PYTHON_SERVER_URL}${pythonEndpoint}`;
      
      const options: any = {
        method,
        headers: {
          'Content-Type': 'application/json'
        }
      };
      
      // Add body for POST requests
      if (method === 'POST') {
        options.body = JSON.stringify(req.body);
      }
      
      const response = await fetch(url, options);
      
      if (!response.ok) {
        throw new Error(`Python server responded with status: ${response.status}`);
      }
      
      const data = await response.json();
      res.json(data);
    } catch (error) {
      console.error(`Error in Python proxy (${pythonEndpoint}):`, error);
      res.status(500).json({ 
        error: `Failed to ${method === 'GET' ? 'fetch' : 'update'} data from Python backend`,
        details: error.message
      });
    }
  };
}

export const pythonProxy = {
  startPythonBackend,
  stopPythonBackend,
  processFile,
  getSourceFiles,
  getProcessedFiles,
  getAnalysisResults,
  getStats,
  getSentimentStats,
  getTopicStats,
  createPythonProxyMiddleware
};