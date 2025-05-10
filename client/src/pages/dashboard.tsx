import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import StatisticsCards from "@/components/statistics-cards";
import AnalyticsCards from "@/components/analytics-cards";
import SourceFilesGrid from "@/components/source-files-grid";
import ProcessedFilesGrid from "@/components/processed-files-grid";
import ProcessingModal from "@/components/processing-modal";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

export default function Dashboard() {
  const [activeTab, setActiveTab] = useState<string>("source");
  const [selectedFiles, setSelectedFiles] = useState<string[]>([]);
  const [isProcessing, setIsProcessing] = useState<boolean>(false);
  const [processingProgress, setProcessingProgress] = useState<Record<string, number>>({
    transcription: 0,
    sentimentAnalysis: 0,
    topicModeling: 0,
    speakerDiarization: 0
  });

  // Fetch dashboard statistics
  const { data: statsData, isLoading: statsLoading } = useQuery({
    queryKey: ['/api/stats'],
    staleTime: 60000, // 1 minute
  });

  // Fetch sentiment distribution
  const { data: sentimentData, isLoading: sentimentLoading } = useQuery({
    queryKey: ['/api/stats/sentiment'],
    staleTime: 300000, // 5 minutes
  });

  // Fetch topic distribution
  const { data: topicsData, isLoading: topicsLoading } = useQuery({
    queryKey: ['/api/stats/topics'],
    staleTime: 300000, // 5 minutes
  });

  const handleSelectFiles = (files: string[]) => {
    setSelectedFiles(files);
  };

  const handleProcessFiles = async () => {
    if (selectedFiles.length === 0) return;
    
    setIsProcessing(true);
    
    // Mock progress updates
    const progressInterval = setInterval(() => {
      setProcessingProgress(prev => {
        const updated = { ...prev };
        
        if (updated.transcription < 100) updated.transcription += 10;
        else if (updated.sentimentAnalysis < 100) updated.sentimentAnalysis += 15;
        else if (updated.topicModeling < 100) updated.topicModeling += 8;
        else if (updated.speakerDiarization < 100) updated.speakerDiarization += 12;
        
        return updated;
      });
    }, 500);

    try {
      await apiRequest('POST', '/api/files/process', { files: selectedFiles });
      
      // Reset state after processing
      setSelectedFiles([]);
      
      // Invalidate queries to refresh data
      queryClient.invalidateQueries({ queryKey: ['/api/files/source'] });
      queryClient.invalidateQueries({ queryKey: ['/api/files/processed'] });
      queryClient.invalidateQueries({ queryKey: ['/api/stats'] });
      
      // Allow progress to finish visually
      setTimeout(() => {
        clearInterval(progressInterval);
        setProcessingProgress({
          transcription: 100,
          sentimentAnalysis: 100,
          topicModeling: 100,
          speakerDiarization: 100
        });
        
        setTimeout(() => {
          setIsProcessing(false);
          setProcessingProgress({
            transcription: 0,
            sentimentAnalysis: 0,
            topicModeling: 0,
            speakerDiarization: 0
          });
        }, 1000);
      }, 1000);
    } catch (error) {
      console.error("Error processing files:", error);
      clearInterval(progressInterval);
      setIsProcessing(false);
      
      // Reset progress
      setProcessingProgress({
        transcription: 0,
        sentimentAnalysis: 0,
        topicModeling: 0,
        speakerDiarization: 0
      });
    }
  };

  return (
    <main className="flex-1 md:ml-64 p-6">
      {/* Dashboard Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-gray-800">Dashboard</h1>
        <div className="flex items-center text-sm text-gray-500">
          <span>Home</span>
          <i className="bi bi-chevron-right mx-2 text-xs"></i>
          <span className="text-primary">Dashboard</span>
        </div>
      </div>

      {/* Statistics Cards */}
      <StatisticsCards 
        isLoading={statsLoading} 
        stats={statsData} 
      />

      {/* Agent Performance Card */}
      <Card className="bg-white mb-6">
        <CardContent className="p-5">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-gray-800">Agent Performance Overview</h2>
            <div className="flex space-x-2">
              <Button variant="outline" size="sm">Weekly</Button>
              <Button variant="default" size="sm">Monthly</Button>
              <Button variant="outline" size="sm">Quarterly</Button>
            </div>
          </div>
          
          {/* Call center image */}
          <div className="h-48 rounded-lg w-full mb-4 bg-gray-100 overflow-hidden">
            <img 
              src="https://images.unsplash.com/photo-1551434678-e076c223a692?ixlib=rb-4.0.3&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=1920&h=600" 
              alt="Call center agents working at desks" 
              className="w-full h-full object-cover"
            />
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="p-4 bg-gray-50 rounded-lg">
              <p className="text-sm text-gray-500">Average Call Time</p>
              <div className="flex items-end mt-2">
                <h3 className="text-xl font-semibold text-gray-800">4:32</h3>
                <span className="text-xs text-success ml-2 mb-1">-0:23</span>
              </div>
              <div className="w-full bg-gray-200 h-1 mt-2 rounded-full">
                <div className="bg-success h-1 rounded-full" style={{width: '65%'}}></div>
              </div>
            </div>
            
            <div className="p-4 bg-gray-50 rounded-lg">
              <p className="text-sm text-gray-500">Call Resolution Rate</p>
              <div className="flex items-end mt-2">
                <h3 className="text-xl font-semibold text-gray-800">78%</h3>
                <span className="text-xs text-success ml-2 mb-1">+5%</span>
              </div>
              <div className="w-full bg-gray-200 h-1 mt-2 rounded-full">
                <div className="bg-primary h-1 rounded-full" style={{width: '78%'}}></div>
              </div>
            </div>
            
            <div className="p-4 bg-gray-50 rounded-lg">
              <p className="text-sm text-gray-500">Customer Satisfaction</p>
              <div className="flex items-end mt-2">
                <h3 className="text-xl font-semibold text-gray-800">4.2/5</h3>
                <span className="text-xs text-success ml-2 mb-1">+0.3</span>
              </div>
              <div className="w-full bg-gray-200 h-1 mt-2 rounded-full">
                <div className="bg-warning h-1 rounded-full" style={{width: '84%'}}></div>
              </div>
            </div>
            
            <div className="p-4 bg-gray-50 rounded-lg">
              <p className="text-sm text-gray-500">Call Transfer Rate</p>
              <div className="flex items-end mt-2">
                <h3 className="text-xl font-semibold text-gray-800">12%</h3>
                <span className="text-xs text-danger ml-2 mb-1">+2%</span>
              </div>
              <div className="w-full bg-gray-200 h-1 mt-2 rounded-full">
                <div className="bg-danger h-1 rounded-full" style={{width: '12%'}}></div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Files Tabs Container */}
      <Tabs value={activeTab} onValueChange={setActiveTab} className="mb-6">
        <TabsList className="border-b border-gray-200 mb-4 bg-transparent">
          <TabsTrigger value="source" className="inline-block py-3 px-4 data-[state=active]:text-primary data-[state=active]:border-b-2 data-[state=active]:border-primary font-medium data-[state=inactive]:text-gray-500">
            Source Files
          </TabsTrigger>
          <TabsTrigger value="processed" className="inline-block py-3 px-4 data-[state=active]:text-primary data-[state=active]:border-b-2 data-[state=active]:border-primary font-medium data-[state=inactive]:text-gray-500">
            Processed Files
          </TabsTrigger>
          <TabsTrigger value="analysis" className="inline-block py-3 px-4 data-[state=active]:text-primary data-[state=active]:border-b-2 data-[state=active]:border-primary font-medium data-[state=inactive]:text-gray-500">
            Analysis Results
          </TabsTrigger>
        </TabsList>

        <TabsContent value="source" className="mt-0">
          <SourceFilesGrid 
            onSelectFiles={handleSelectFiles} 
            selectedFiles={selectedFiles}
            onProcessFiles={handleProcessFiles}
          />
        </TabsContent>
        
        <TabsContent value="processed" className="mt-0">
          <ProcessedFilesGrid />
        </TabsContent>
        
        <TabsContent value="analysis" className="mt-0">
          <AnalyticsCards 
            sentimentData={sentimentData}
            topicsData={topicsData}
            sentimentLoading={sentimentLoading}
            topicsLoading={topicsLoading}
          />
        </TabsContent>
      </Tabs>

      {/* Processing Modal */}
      {isProcessing && (
        <ProcessingModal 
          isOpen={isProcessing}
          onClose={() => setIsProcessing(false)}
          progress={processingProgress}
        />
      )}
    </main>
  );
}
