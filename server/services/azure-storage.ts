import { BlobServiceClient, StorageSharedKeyCredential, ContainerClient } from "@azure/storage-blob";
import { azureConfig } from "@shared/schema";

// Azure Storage Account credentials
const storageAccountUrl = process.env.AZURE_STORAGE_ACCOUNT_URL || azureConfig.storageAccountUrl;
const storageAccountKey = process.env.AZURE_STORAGE_ACCOUNT_KEY || azureConfig.storageAccountKey;
const sourceContainerName = process.env.AZURE_SOURCE_CONTAINER_NAME || azureConfig.sourceContainerName;
const destinationContainerName = process.env.AZURE_DESTINATION_CONTAINER_NAME || azureConfig.destinationContainerName;

// Create the BlobServiceClient object
const sharedKeyCredential = new StorageSharedKeyCredential(
  storageAccountUrl.split('.')[0].replace('https://', ''),
  storageAccountKey
);

const blobServiceClient = new BlobServiceClient(
  storageAccountUrl,
  sharedKeyCredential
);

// Get container clients
const sourceContainerClient = blobServiceClient.getContainerClient(sourceContainerName);
const destinationContainerClient = blobServiceClient.getContainerClient(destinationContainerName);

export async function getSourceFiles() {
  try {
    const files = [];
    
    // List blobs in the source container
    for await (const blob of sourceContainerClient.listBlobsFlat()) {
      const blobClient = sourceContainerClient.getBlobClient(blob.name);
      const properties = await blobClient.getProperties();
      
      files.push({
        name: blob.name,
        url: blobClient.url,
        size: properties.contentLength,
        lastModified: properties.lastModified,
        contentType: properties.contentType
      });
    }
    
    return files;
  } catch (error) {
    console.error("Error getting source files:", error);
    throw error;
  }
}

export async function getProcessedFiles() {
  try {
    const files = [];
    
    // List blobs in the destination container
    for await (const blob of destinationContainerClient.listBlobsFlat()) {
      const blobClient = destinationContainerClient.getBlobClient(blob.name);
      const properties = await blobClient.getProperties();
      
      files.push({
        name: blob.name,
        url: blobClient.url,
        size: properties.contentLength,
        lastModified: properties.lastModified,
        contentType: properties.contentType
      });
    }
    
    return files;
  } catch (error) {
    console.error("Error getting processed files:", error);
    throw error;
  }
}

export async function moveFileToProcessed(fileName: string) {
  try {
    // Get source blob
    const sourceBlobClient = sourceContainerClient.getBlobClient(fileName);
    
    // Check if source blob exists
    if (!await sourceBlobClient.exists()) {
      throw new Error(`Source blob ${fileName} does not exist`);
    }
    
    // Get destination blob
    const destinationBlobClient = destinationContainerClient.getBlobClient(fileName);
    
    // Download source blob
    const downloadResponse = await sourceBlobClient.download();
    const sourceContent = await streamToBuffer(downloadResponse.readableStreamBody);
    
    // Upload to destination
    await destinationBlobClient.upload(sourceContent, sourceContent.length);
    
    // Delete source blob
    await sourceBlobClient.delete();
    
    return true;
  } catch (error) {
    console.error(`Error moving file ${fileName} to processed:`, error);
    throw error;
  }
}

export async function downloadBlob(containerClient: ContainerClient, blobName: string): Promise<Buffer> {
  const blobClient = containerClient.getBlobClient(blobName);
  const downloadResponse = await blobClient.download();
  return await streamToBuffer(downloadResponse.readableStreamBody);
}

// Helper to convert a ReadableStream to Buffer
async function streamToBuffer(readableStream: NodeJS.ReadableStream | undefined): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    if (!readableStream) {
      reject(new Error("Readable stream is undefined"));
      return;
    }
    
    readableStream.on('data', (data) => {
      chunks.push(Buffer.isBuffer(data) ? data : Buffer.from(data));
    });
    
    readableStream.on('end', () => {
      resolve(Buffer.concat(chunks));
    });
    
    readableStream.on('error', reject);
  });
}

export { sourceContainerClient, destinationContainerClient };
