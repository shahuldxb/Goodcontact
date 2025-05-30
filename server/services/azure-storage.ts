import { BlobServiceClient, StorageSharedKeyCredential, ContainerClient, BlobSASPermissions, generateBlobSASQueryParameters } from "@azure/storage-blob";
import { azureConfig } from "@shared/schema";

/**
 * Azure Storage Service - NestJS-inspired service class for Azure Blob Storage operations
 */
class AzureStorageService {
  private storageAccountUrl: string;
  private storageAccountKey: string;
  private accountName: string;
  private sharedKeyCredential: StorageSharedKeyCredential;
  private blobServiceClient: BlobServiceClient;
  private sourceContainerName: string;
  private destinationContainerName: string;
  private sourceContainerClient: ContainerClient;
  private destinationContainerClient: ContainerClient;

  constructor() {
    // Initialize credentials from environment or config
    this.storageAccountUrl = process.env.AZURE_STORAGE_ACCOUNT_URL || azureConfig.storageAccountUrl;
    this.storageAccountKey = process.env.AZURE_STORAGE_ACCOUNT_KEY || azureConfig.storageAccountKey;
    this.sourceContainerName = process.env.AZURE_SOURCE_CONTAINER_NAME || azureConfig.sourceContainerName;
    this.destinationContainerName = process.env.AZURE_DESTINATION_CONTAINER_NAME || azureConfig.destinationContainerName;
    
    // Extract account name from URL
    this.accountName = this.storageAccountUrl.split('.')[0].replace('https://', '');
    
    // Initialize shared key credential
    this.sharedKeyCredential = new StorageSharedKeyCredential(
      this.accountName,
      this.storageAccountKey
    );
    
    // Initialize blob service client
    this.blobServiceClient = new BlobServiceClient(
      this.storageAccountUrl,
      this.sharedKeyCredential
    );
    
    // Get container clients
    this.sourceContainerClient = this.blobServiceClient.getContainerClient(this.sourceContainerName);
    this.destinationContainerClient = this.blobServiceClient.getContainerClient(this.destinationContainerName);
  }

  /**
   * Get the blob service client
   * @returns The BlobServiceClient instance
   */
  getBlobServiceClient(): BlobServiceClient {
    return this.blobServiceClient;
  }
  
  /**
   * Get the source container client
   * @returns The source ContainerClient instance
   */
  getSourceContainerClient(): ContainerClient {
    return this.sourceContainerClient;
  }
  
  /**
   * Get the destination container client
   * @returns The destination ContainerClient instance
   */
  getDestinationContainerClient(): ContainerClient {
    return this.destinationContainerClient;
  }
}

// Initialize the service as a singleton
const azureStorageService = new AzureStorageService();

// Export container clients for backward compatibility
const sourceContainerClient = azureStorageService.getSourceContainerClient();
const destinationContainerClient = azureStorageService.getDestinationContainerClient();

/**
 * Get files from the source container
 * @returns Array of blob details
 */
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

/**
 * Get files from the processed/destination container
 * @returns Array of processed blob details
 */
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
    const blockBlobClient = destinationBlobClient.getBlockBlobClient();
    await blockBlobClient.upload(sourceContent, sourceContent.length);
    
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

/**
 * Generate a SAS URL for a blob with specified permissions
 * 
 * @param containerName - The container name
 * @param blobName - The blob name
 * @param permissions - The permissions to grant (default: read)
 * @param expiryHours - Hours until expiration (default: 1 hour)
 * @returns SAS URL for the blob
 */
AzureStorageService.prototype.generateSasUrl = function(
  containerName: string,
  blobName: string,
  permissions: BlobSASPermissions = BlobSASPermissions.parse("r"),
  expiryHours: number = 1
): string {
  try {
    // Get container client
    const containerClient = this.blobServiceClient.getContainerClient(containerName);
    const blobClient = containerClient.getBlobClient(blobName);
    
    // Set start time 5 minutes before current time to avoid clock skew issues
    const startDate = new Date();
    startDate.setMinutes(startDate.getMinutes() - 5);
    
    // Set expiry time
    const expiryDate = new Date();
    expiryDate.setHours(expiryDate.getHours() + expiryHours);
    
    // Generate SAS token
    const sasToken = generateBlobSASQueryParameters({
      containerName,
      blobName,
      permissions,
      startsOn: startDate,
      expiresOn: expiryDate,
    }, this.sharedKeyCredential).toString();
    
    // Return the full URL with SAS token
    return `${blobClient.url}?${sasToken}`;
  } catch (error) {
    console.error(`Error generating SAS URL for ${containerName}/${blobName}:`, error);
    throw error;
  }
};

/**
 * Generate a SAS URL for a blob with specified permissions
 * 
 * @param containerName - The container name
 * @param blobName - The blob name
 * @param permissions - The permissions to grant (default: read)
 * @param expiryHours - Hours until expiration (default: 1 hour)
 * @returns SAS URL for the blob
 */
export function generateSasUrl(
  containerName: string,
  blobName: string,
  permissions: BlobSASPermissions = BlobSASPermissions.parse("r"),
  expiryHours: number = 1
): string {
  return azureStorageService.generateSasUrl(containerName, blobName, permissions, expiryHours);
}

/**
 * Generate a SAS URL with read permissions for a source container blob
 * 
 * @param blobName - The blob name in the source container
 * @param expiryHours - Hours until expiration (default: 1 hour)
 * @returns SAS URL for the blob
 */
export function generateSourceBlobSasUrl(blobName: string, expiryHours: number = 1): string {
  return generateSasUrl(azureStorageService.sourceContainerName, blobName, BlobSASPermissions.parse("r"), expiryHours);
}

/**
 * Generate a SAS URL with read permissions for a destination container blob
 * 
 * @param blobName - The blob name in the destination container
 * @param expiryHours - Hours until expiration (default: 1 hour)
 * @returns SAS URL for the blob
 */
export function generateDestinationBlobSasUrl(blobName: string, expiryHours: number = 1): string {
  return generateSasUrl(azureStorageService.destinationContainerName, blobName, BlobSASPermissions.parse("r"), expiryHours);
}

/**
 * Create a container if it doesn't exist
 * 
 * @param containerName - The container name to create
 * @returns True if container was created or already exists
 */
export async function createContainerIfNotExists(containerName: string): Promise<boolean> {
  try {
    const containerClient = blobServiceClient.getContainerClient(containerName);
    const createResult = await containerClient.createIfNotExists();
    
    if (createResult.succeeded) {
      console.log(`Container '${containerName}' created successfully`);
    } else {
      console.log(`Container '${containerName}' already exists`);
    }
    
    return true;
  } catch (error) {
    console.error(`Error creating container '${containerName}':`, error);
    return false;
  }
}

export { sourceContainerClient, destinationContainerClient };
