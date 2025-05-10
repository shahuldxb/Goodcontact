import { useState, useEffect } from "react";
import { useRoute } from "wouter";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import CallDetails from "@/components/call-details";

export default function Analysis() {
  const [_, params] = useRoute("/analysis/:fileid");
  const [activeTab, setActiveTab] = useState<string>("transcription");
  
  // If no fileid in URL, fetch the most recent file
  const fileid = params?.fileid;

  // Fetch file analysis results
  const { data: analysisData, isLoading } = useQuery({
    queryKey: ['/api/analysis', fileid],
    enabled: !!fileid
  });

  // Fetch all processed files for selection
  const { data: processedFilesData } = useQuery({
    queryKey: ['/api/files/processed']
  });

  if (!fileid) {
    return (
      <main className="flex-1 md:ml-64 p-6">
        <div className="mb-6">
          <h1 className="text-2xl font-semibold text-gray-800">Analysis</h1>
          <div className="flex items-center text-sm text-gray-500">
            <span>Home</span>
            <i className="bi bi-chevron-right mx-2 text-xs"></i>
            <span className="text-primary">Analysis</span>
          </div>
        </div>

        <Card className="bg-white p-6 text-center">
          <h2 className="text-xl font-semibold mb-4">No File Selected</h2>
          <p className="text-gray-600 mb-6">
            Please select a processed file to view analysis details.
          </p>
          
          {processedFilesData?.files?.length > 0 ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {processedFilesData.files.slice(0, 6).map((file: any) => (
                <Card key={file.name} className="cursor-pointer hover:shadow-md transition-shadow">
                  <CardContent className="p-4">
                    <a href={`/analysis/${file.fileid || 'placeholder-id'}`} className="block">
                      <h3 className="font-medium text-gray-800 truncate">{file.name}</h3>
                      <p className="text-sm text-gray-500">{new Date(file.lastModified).toLocaleDateString()}</p>
                    </a>
                  </CardContent>
                </Card>
              ))}
            </div>
          ) : (
            <p className="text-gray-500">No processed files available. Process some files from the Dashboard.</p>
          )}
        </Card>
      </main>
    );
  }

  return (
    <main className="flex-1 md:ml-64 p-6">
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-gray-800">Analysis Details</h1>
        <div className="flex items-center text-sm text-gray-500">
          <span>Home</span>
          <i className="bi bi-chevron-right mx-2 text-xs"></i>
          <span>Analysis</span>
          <i className="bi bi-chevron-right mx-2 text-xs"></i>
          <span className="text-primary">{analysisData?.results?.asset?.filename || "Loading..."}</span>
        </div>
      </div>

      <Tabs defaultValue="transcription" value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="border-b border-gray-200 mb-4 bg-transparent">
          <TabsTrigger value="transcription" className="inline-block py-3 px-4 data-[state=active]:text-primary data-[state=active]:border-b-2 data-[state=active]:border-primary font-medium data-[state=inactive]:text-gray-500">
            Transcription
          </TabsTrigger>
          <TabsTrigger value="sentiment" className="inline-block py-3 px-4 data-[state=active]:text-primary data-[state=active]:border-b-2 data-[state=active]:border-primary font-medium data-[state=inactive]:text-gray-500">
            Sentiment Analysis
          </TabsTrigger>
          <TabsTrigger value="topics" className="inline-block py-3 px-4 data-[state=active]:text-primary data-[state=active]:border-b-2 data-[state=active]:border-primary font-medium data-[state=inactive]:text-gray-500">
            Topic Modeling
          </TabsTrigger>
          <TabsTrigger value="speakers" className="inline-block py-3 px-4 data-[state=active]:text-primary data-[state=active]:border-b-2 data-[state=active]:border-primary font-medium data-[state=inactive]:text-gray-500">
            Speaker Diarization
          </TabsTrigger>
          <TabsTrigger value="forbidden" className="inline-block py-3 px-4 data-[state=active]:text-primary data-[state=active]:border-b-2 data-[state=active]:border-primary font-medium data-[state=inactive]:text-gray-500">
            Forbidden Phrases
          </TabsTrigger>
        </TabsList>

        {isLoading ? (
          <Card className="bg-white">
            <CardContent className="p-5">
              <div className="space-y-4">
                <Skeleton className="h-8 w-3/4" />
                <Skeleton className="h-4 w-full" />
                <Skeleton className="h-4 w-full" />
                <Skeleton className="h-4 w-4/5" />
                <Skeleton className="h-32 w-full" />
              </div>
            </CardContent>
          </Card>
        ) : (
          <CallDetails
            data={analysisData?.results}
            activeTab={activeTab}
          />
        )}
      </Tabs>
    </main>
  );
}
