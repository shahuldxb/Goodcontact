import React, { useState, useEffect } from 'react';
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Separator } from "@/components/ui/separator";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";

interface TestResultFile {
  filename: string;
  size: number;
  created: string;
}

interface TestResult {
  blob_name: string;
  timestamp: string;
  execution_time_seconds: number;
  result: any;
  formatted_transcript?: string;
}

export function DirectTestResults() {
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [testFileName, setTestFileName] = useState("agricultural_leasing_(ijarah)_normal.mp3");
  const [transcriptionMethod, setTranscriptionMethod] = useState("shortcut");
  const [currentMethod, setCurrentMethod] = useState("");
  const [resultFiles, setResultFiles] = useState<TestResultFile[]>([]);
  const [selectedResult, setSelectedResult] = useState<TestResult | null>(null);
  const [currentTab, setCurrentTab] = useState("run");
  const [formattedTranscript, setFormattedTranscript] = useState("");
  const [fileSource, setFileSource] = useState<'azure' | 'local'>('azure');
  const [localFile, setLocalFile] = useState<File | null>(null);
  
  interface AzureFile {
    name: string;
    size: number;
    url: string;
    lastModified: string;
    contentType: string;
  }
  
  const [sourceFiles, setSourceFiles] = useState<AzureFile[]>([]);
  const [loadingSourceFiles, setLoadingSourceFiles] = useState(false);
  const { toast } = useToast();

  // Load test result files and get current transcription method on component mount
  useEffect(() => {
    fetchResultFiles();
    fetchCurrentMethod();
    fetchSourceFiles();
  }, []);
  
  // Fetch source files from Azure Storage
  const fetchSourceFiles = async () => {
    if (fileSource !== 'azure') return;
    
    setLoadingSourceFiles(true);
    try {
      const response = await fetch('/api/files/source');
      const data = await response.json();
      
      if (data.files && Array.isArray(data.files)) {
        setSourceFiles(data.files);
      } else {
        setSourceFiles([]);
      }
    } catch (error) {
      console.error("Error fetching source files:", error);
      toast({
        title: 'Error',
        description: 'Failed to load Azure Storage files',
        variant: 'destructive',
      });
      setSourceFiles([]);
    } finally {
      setLoadingSourceFiles(false);
    }
  };
  
  // Refetch source files when file source changes to Azure
  useEffect(() => {
    if (fileSource === 'azure') {
      fetchSourceFiles();
    }
  }, [fileSource]);
  
  // Fetch the current transcription method
  const fetchCurrentMethod = async () => {
    try {
      const response = await fetch('/api/config/transcription-method');
      const data = await response.json();
      setCurrentMethod(data.current_method || "");
      // Set the state value to match the current setting
      setTranscriptionMethod(data.current_method || "shortcut");
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Failed to get current transcription method',
        variant: 'destructive',
      });
    }
  };
  
  // Update the transcription method on the server
  const updateTranscriptionMethod = async (method: string) => {
    try {
      const response = await fetch('/api/config/transcription-method', {
        method: 'POST',
        body: JSON.stringify({ method }),
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      const data = await response.json();
      if (data.status === 'success') {
        setCurrentMethod(data.current_method);
        toast({
          title: 'Success',
          description: `Transcription method set to ${data.current_method}`,
        });
      } else {
        toast({
          title: 'Error',
          description: data.message || 'Failed to update transcription method',
          variant: 'destructive',
        });
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Failed to update transcription method',
        variant: 'destructive',
      });
    }
  };

  const fetchResultFiles = async () => {
    setLoading(true);
    try {
      const response = await fetch('/debug/direct-test-results');
      const data = await response.json();
      
      if (data.status === 'success') {
        setResultFiles(data.files || []);
      } else {
        toast({
          title: 'Error',
          description: data.message || 'Failed to load test result files',
          variant: 'destructive',
        });
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Failed to load test result files',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const runTest = async () => {
    // For Azure Storage, validate filename
    if (fileSource === 'azure' && !testFileName.trim()) {
      toast({
        title: 'Error',
        description: 'Please enter a file name',
        variant: 'destructive',
      });
      return;
    }
    
    // For local file upload, validate file
    if (fileSource === 'local' && !localFile) {
      toast({
        title: 'Error',
        description: 'Please select a file to upload',
        variant: 'destructive',
      });
      return;
    }

    // Update the transcription method if needed before running the test
    if (transcriptionMethod !== currentMethod) {
      await updateTranscriptionMethod(transcriptionMethod);
    }

    setRunning(true);
    try {
      let response;
      let data;
      
      if (fileSource === 'azure') {
        // For Azure storage, use the existing API endpoint
        response = await fetch(
          `/debug/direct-transcription?test_file=${encodeURIComponent(testFileName)}`
        );
        data = await response.json();
      } else {
        // For local file upload, use FormData to send the file
        const formData = new FormData();
        formData.append('file', localFile as File);
        
        response = await fetch('/debug/direct-transcription-upload', {
          method: 'POST',
          body: formData,
        });
        data = await response.json();
      }
      
      if (data.status === 'success') {
        const fileName = fileSource === 'azure' ? testFileName : (localFile as File).name;
        toast({
          title: 'Success',
          description: `Test completed for ${fileName} using ${transcriptionMethod} method`,
        });
        
        // Set the formatted transcript if available
        if (data.formatted_transcript) {
          setFormattedTranscript(data.formatted_transcript);
        }
        
        // Refresh the list of result files
        fetchResultFiles();
        setCurrentTab("results");
      } else {
        toast({
          title: 'Error',
          description: data.message || 'Test failed',
          variant: 'destructive',
        });
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Test failed',
        variant: 'destructive',
      });
    } finally {
      setRunning(false);
    }
  };

  const viewResult = async (filename: string) => {
    setLoading(true);
    try {
      const response = await fetch(`/debug/direct-test-result?filename=${encodeURIComponent(filename)}`);
      const data = await response.json();
      
      if (data.status === 'success') {
        setSelectedResult({
          blob_name: data.filename,
          timestamp: data.timestamp,
          execution_time_seconds: data.result.execution_time || 0,
          result: data.result,
          formatted_transcript: data.formatted_transcript
        });
        
        // Set formatted transcript if available
        if (data.formatted_transcript) {
          setFormattedTranscript(data.formatted_transcript);
        } else if (data.result?.transcript) {
          setFormattedTranscript(data.result.transcript);
        } else {
          setFormattedTranscript('No transcript available');
        }
      } else {
        toast({
          title: 'Error',
          description: data.message || 'Failed to load test result',
          variant: 'destructive',
        });
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Failed to load test result',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const formatDate = (dateString: string) => {
    try {
      const date = new Date(dateString);
      return date.toLocaleString();
    } catch (e) {
      return dateString;
    }
  };

  const formatSize = (bytes: number) => {
    const kb = bytes / 1024;
    if (kb < 1024) {
      return `${kb.toFixed(2)} KB`;
    }
    const mb = kb / 1024;
    return `${mb.toFixed(2)} MB`;
  };

  return (
    <div className="w-full max-w-7xl mx-auto py-4 px-4">
      <h1 className="text-2xl font-bold mb-4">Direct Transcription Test Results</h1>
      
      <Tabs defaultValue="run" value={currentTab} onValueChange={setCurrentTab}>
        <TabsList className="grid w-full grid-cols-2 mb-4">
          <TabsTrigger value="run">Run Test</TabsTrigger>
          <TabsTrigger value="results">View Results</TabsTrigger>
        </TabsList>
        
        <TabsContent value="run">
          <Card>
            <CardHeader>
              <CardTitle>Run Direct Transcription Test</CardTitle>
              <CardDescription>
                Test the direct transcription function with a file from Azure Blob Storage or upload a local file
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="space-y-4 mb-4">
                  <div className="bg-secondary/20 p-4 rounded-lg border">
                    <h3 className="text-lg font-semibold mb-2">File Source</h3>
                    <RadioGroup 
                      value={fileSource}
                      onValueChange={(value) => {
                        setFileSource(value as 'azure' | 'local');
                        // Clear selected file when switching modes
                        if (value === 'azure') {
                          setLocalFile(null);
                        }
                      }}
                      className="flex space-x-6"
                    >
                      <div className="flex items-center space-x-2">
                        <RadioGroupItem value="azure" id="azure" />
                        <Label htmlFor="azure" className="font-normal">Azure Storage (shahulin container)</Label>
                      </div>
                      <div className="flex items-center space-x-2">
                        <RadioGroupItem value="local" id="local" />
                        <Label htmlFor="local" className="font-normal">Local File Upload</Label>
                      </div>
                    </RadioGroup>
                  </div>
                  
                  {fileSource === 'azure' ? (
                    <div className="bg-primary/5 p-4 rounded-lg border border-primary/20">
                      <h3 className="font-medium mb-2">Azure Blob File (shahulin container)</h3>
                      <div className="space-y-4">
                        <div>
                          <Label htmlFor="filename" className="mb-2 block">Blob File Name</Label>
                          <div className="flex space-x-2">
                            <Input
                              id="filename"
                              placeholder="Enter blob file name (e.g., audio_sample.mp3)"
                              value={testFileName}
                              onChange={(e) => setTestFileName(e.target.value)}
                              className="bg-white flex-1"
                            />
                            <Button 
                              variant="outline" 
                              type="button"
                              onClick={fetchSourceFiles}
                              className="whitespace-nowrap"
                              disabled={loadingSourceFiles}
                            >
                              {loadingSourceFiles ? 'Loading...' : 'Refresh Files'}
                            </Button>
                          </div>
                        </div>
                        
                        <div>
                          <Label className="mb-1 block">Available Files</Label>
                          {loadingSourceFiles ? (
                            <div className="py-2">
                              <div className="flex items-center space-x-2">
                                <span className="inline-block w-4 h-4 border-2 border-t-transparent border-primary rounded-full animate-spin"></span>
                                <span>Loading files from Azure Storage...</span>
                              </div>
                            </div>
                          ) : sourceFiles.length > 0 ? (
                            <div className="border rounded-md h-48 overflow-y-auto bg-white">
                              <div className="p-1">
                                {sourceFiles.map((file: AzureFile, index) => (
                                  <button
                                    key={index}
                                    className={`w-full text-left px-3 py-2 rounded text-sm transition-colors ${testFileName === file.name ? 'bg-primary text-white' : 'hover:bg-secondary'}`}
                                    onClick={() => setTestFileName(file.name)}
                                  >
                                    <div className="flex justify-between items-center">
                                      <span className="truncate mr-2">{file.name}</span>
                                      <span className="text-xs text-muted-foreground whitespace-nowrap">
                                        {(file.size / 1024 / 1024).toFixed(2)} MB
                                      </span>
                                    </div>
                                  </button>
                                ))}
                              </div>
                            </div>
                          ) : (
                            <div className="text-center p-4 border rounded bg-secondary/20">
                              <p>No files found in the 'shahulin' container</p>
                              <Button 
                                variant="ghost" 
                                size="sm" 
                                onClick={fetchSourceFiles} 
                                className="mt-2"
                              >
                                Retry
                              </Button>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="bg-primary/5 p-4 rounded-lg border border-primary/20">
                      <h3 className="font-medium mb-2">Local Audio File</h3>
                      <div className="space-y-2">
                        <Label htmlFor="fileUpload">Select Audio File</Label>
                        <Input
                          id="fileUpload"
                          type="file"
                          accept="audio/*"
                          onChange={(e) => setLocalFile(e.target.files ? e.target.files[0] : null)}
                          className="bg-white"
                        />
                        {localFile && (
                          <div className="text-sm p-2 bg-secondary/30 rounded mt-2">
                            <div className="font-medium">Selected file:</div>
                            <div>{localFile.name}</div>
                            <div className="text-xs text-muted-foreground">
                              Size: {(localFile.size / 1024 / 1024).toFixed(2)} MB • 
                              Type: {localFile.type || 'Unknown'}
                            </div>
                          </div>
                        )}
                        <p className="text-xs text-muted-foreground">
                          Upload an audio file from your computer for transcription testing.
                        </p>
                      </div>
                    </div>
                  )}
                </div>
                
                <div className="bg-secondary/20 p-4 rounded-lg border">
                  <h3 className="text-lg font-semibold mb-2">Transcription Method</h3>
                  <p className="text-sm text-muted-foreground mb-3">
                    Select which Deepgram API implementation to use for the transcription.
                  </p>
                  <RadioGroup 
                    value={transcriptionMethod}
                    onValueChange={setTranscriptionMethod}
                    className="flex flex-col space-y-2"
                  >
                    <div className="flex items-center space-x-2 p-2 rounded hover:bg-secondary/30">
                      <RadioGroupItem value="shortcut" id="shortcut" />
                      <div>
                        <Label htmlFor="shortcut" className="font-medium">Shortcut</Label>
                        <p className="text-xs text-muted-foreground">Fastest method, uses direct implementation for Azure blobs.</p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-2 p-2 rounded hover:bg-secondary/30">
                      <RadioGroupItem value="direct" id="direct" />
                      <div>
                        <Label htmlFor="direct" className="font-medium">Direct REST API</Label>
                        <p className="text-xs text-muted-foreground">Uses raw HTTP requests to Deepgram's API endpoints.</p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-2 p-2 rounded hover:bg-secondary/30">
                      <RadioGroupItem value="rest_api" id="rest_api" />
                      <div>
                        <Label htmlFor="rest_api" className="font-medium">REST API Client</Label>
                        <p className="text-xs text-muted-foreground">Uses Python requests with the Deepgram REST API.</p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-2 p-2 rounded hover:bg-secondary/30">
                      <RadioGroupItem value="sdk" id="sdk" />
                      <div>
                        <Label htmlFor="sdk" className="font-medium">Deepgram SDK</Label>
                        <p className="text-xs text-muted-foreground">Uses the official Deepgram Python SDK.</p>
                      </div>
                    </div>
                  </RadioGroup>
                  
                  {currentMethod && (
                    <div className="mt-3 p-2 bg-muted rounded border border-muted-foreground/20">
                      <div className="text-sm font-medium">Current System Setting:</div>
                      <div className="text-sm">{currentMethod}</div>
                      <div className="text-xs text-muted-foreground mt-1">
                        This will be updated when you run a test with a different method.
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </CardContent>
            <CardFooter className="flex justify-between items-center border-t pt-6">
              <div className="text-sm text-muted-foreground">
                {fileSource === 'azure' ? 
                  `Selected file: ${testFileName}` : 
                  (localFile ? `Selected file: ${localFile.name}` : 'No file selected')}
              </div>
              <Button 
                onClick={runTest} 
                disabled={running} 
                size="lg"
                className="bg-primary hover:bg-primary/90"
              >
                {running ? (
                  <>
                    <span className="animate-pulse mr-2">Processing</span>
                    <span className="inline-block w-4 h-4 border-2 border-t-transparent border-white rounded-full animate-spin"></span>
                  </>
                ) : (
                  'Run Transcription Test'
                )}
              </Button>
            </CardFooter>
          </Card>
        </TabsContent>
        
        <TabsContent value="results">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Left panel: List of result files */}
            <Card className="lg:col-span-1 h-fit">
              <CardHeader className="pb-2">
                <CardTitle>Test Results</CardTitle>
                <CardDescription>
                  {resultFiles.length} result files found
                </CardDescription>
              </CardHeader>
              <CardContent>
                {loading ? (
                  <div className="space-y-2">
                    <Skeleton className="h-4 w-full" />
                    <Skeleton className="h-4 w-full" />
                    <Skeleton className="h-4 w-full" />
                  </div>
                ) : resultFiles.length === 0 ? (
                  <Alert>
                    <AlertTitle>No Results</AlertTitle>
                    <AlertDescription>
                      No test results found. Run a test first.
                    </AlertDescription>
                  </Alert>
                ) : (
                  <div className="space-y-2 max-h-[300px] overflow-y-auto">
                    {resultFiles.map((file) => (
                      <div 
                        key={file.filename}
                        className="p-2 border rounded cursor-pointer hover:bg-slate-100 dark:hover:bg-slate-800"
                        onClick={() => viewResult(file.filename)}
                      >
                        <div className="font-medium truncate">{file.filename}</div>
                        <div className="text-sm text-muted-foreground">
                          {formatSize(file.size)} • {formatDate(file.created)}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
              <CardFooter>
                <Button 
                  variant="outline" 
                  size="sm"
                  onClick={fetchResultFiles}
                  disabled={loading}
                >
                  Refresh
                </Button>
              </CardFooter>
            </Card>
            
            {/* Right panel: Result details */}
            <Card className="lg:col-span-2">
              <CardHeader className="pb-2">
                <CardTitle>Result Details</CardTitle>
                {selectedResult && (
                  <CardDescription>
                    {selectedResult.blob_name} • {formatDate(selectedResult.timestamp)} • 
                    {selectedResult.execution_time_seconds.toFixed(2)}s
                  </CardDescription>
                )}
              </CardHeader>
              <CardContent>
                {loading ? (
                  <div className="space-y-2">
                    <Skeleton className="h-4 w-full" />
                    <Skeleton className="h-4 w-full" />
                    <Skeleton className="h-4 w-3/4" />
                  </div>
                ) : selectedResult ? (
                  <div className="space-y-4">
                    <div>
                      <h3 className="font-semibold mb-2">Formatted Transcript</h3>
                      <div className="p-4 border rounded bg-slate-50 dark:bg-slate-900 max-h-[300px] overflow-y-auto font-mono whitespace-pre-line">
                        {formattedTranscript || 'No formatted transcript available'}
                      </div>
                    </div>
                    
                    <div>
                      <h3 className="font-semibold mb-2">Raw Transcript</h3>
                      <div className="p-2 border rounded bg-slate-50 dark:bg-slate-900 max-h-[150px] overflow-y-auto">
                        {selectedResult.result?.transcript || 'No transcript found'}
                      </div>
                    </div>
                    
                    <Separator />
                    
                    <div>
                      <h3 className="font-semibold mb-2">Raw Response</h3>
                      <div className="p-2 border rounded bg-slate-50 dark:bg-slate-900 max-h-[300px] overflow-y-auto">
                        <pre className="text-xs">
                          {JSON.stringify(selectedResult.result, null, 2)}
                        </pre>
                      </div>
                    </div>
                  </div>
                ) : (
                  <Alert>
                    <AlertTitle>No Result Selected</AlertTitle>
                    <AlertDescription>
                      Select a test result from the list to view details.
                    </AlertDescription>
                  </Alert>
                )}
              </CardContent>
            </Card>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}

export default DirectTestResults;