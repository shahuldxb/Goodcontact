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
  const { toast } = useToast();

  // Load test result files and get current transcription method on component mount
  useEffect(() => {
    fetchResultFiles();
    fetchCurrentMethod();
  }, []);
  
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
    if (!testFileName.trim()) {
      toast({
        title: 'Error',
        description: 'Please enter a file name',
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
      const response = await fetch(
        `/debug/direct-transcription?test_file=${encodeURIComponent(testFileName)}`
      );
      const data = await response.json();
      
      if (data.status === 'success') {
        toast({
          title: 'Success',
          description: `Test completed for ${testFileName} using ${transcriptionMethod} method`,
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
      const response = await apiRequest(`/api/debug/direct-test-results/${filename}`);
      if (response.status === 'success') {
        setSelectedResult(response.result);
        
        // Set formatted transcript if available
        if (response.result?.formatted_transcript) {
          setFormattedTranscript(response.result.formatted_transcript);
        } else if (response.result?.transcript) {
          setFormattedTranscript(response.result.transcript);
        } else {
          setFormattedTranscript('No transcript available');
        }
      } else {
        toast({
          title: 'Error',
          description: response.message || 'Failed to load test result',
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
                Test the direct transcription function with a file from Azure Blob Storage
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="filename">Blob File Name</Label>
                  <Input
                    id="filename"
                    placeholder="Enter blob file name (e.g., audio_sample.mp3)"
                    value={testFileName}
                    onChange={(e) => setTestFileName(e.target.value)}
                  />
                </div>
                
                <div className="space-y-2">
                  <Label>Transcription Method</Label>
                  <RadioGroup 
                    value={transcriptionMethod}
                    onValueChange={setTranscriptionMethod}
                    className="flex flex-col space-y-1"
                  >
                    <div className="flex items-center space-x-2">
                      <RadioGroupItem value="shortcut" id="shortcut" />
                      <Label htmlFor="shortcut" className="font-normal">Shortcut (Fastest)</Label>
                    </div>
                    <div className="flex items-center space-x-2">
                      <RadioGroupItem value="direct" id="direct" />
                      <Label htmlFor="direct" className="font-normal">Direct REST API</Label>
                    </div>
                    <div className="flex items-center space-x-2">
                      <RadioGroupItem value="rest_api" id="rest_api" />
                      <Label htmlFor="rest_api" className="font-normal">REST API Client</Label>
                    </div>
                    <div className="flex items-center space-x-2">
                      <RadioGroupItem value="sdk" id="sdk" />
                      <Label htmlFor="sdk" className="font-normal">SDK</Label>
                    </div>
                  </RadioGroup>
                </div>
                
                {currentMethod && (
                  <Alert className="bg-muted">
                    <AlertTitle>Current Setting</AlertTitle>
                    <AlertDescription>
                      The current transcription method is set to: <span className="font-semibold">{currentMethod}</span>
                    </AlertDescription>
                  </Alert>
                )}
              </div>
            </CardContent>
            <CardFooter>
              <Button onClick={runTest} disabled={running}>
                {running ? 'Running Test...' : 'Run Test'}
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