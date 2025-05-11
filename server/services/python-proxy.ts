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
export const pythonProxy = {
  startPythonBackend,
  stopPythonBackend,
  processFile,
  getSourceFiles,
  getProcessedFiles,
  getAnalysisResults,
  getStats,
  getSentimentStats,
  getTopicStats
};