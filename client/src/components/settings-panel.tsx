import React, { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { apiRequest } from '@/lib/queryClient';
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Button } from '@/components/ui/button';
import { useToast } from '@/hooks/use-toast';
import { Loader2 } from 'lucide-react';

interface TranscriptionConfig {
  current_method: string;
  available_methods: string[];
}

export function SettingsPanel() {
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const [selectedMethod, setSelectedMethod] = useState<string>('');

  // Fetch current transcription method config
  const { data, isLoading, error } = useQuery<TranscriptionConfig>({
    queryKey: ['/api/config/transcription-method'],
    staleTime: 5 * 60 * 1000, // 5 minutes
  });

  // Set selected method when data is loaded
  useEffect(() => {
    if (data?.current_method) {
      setSelectedMethod(data.current_method);
    }
  }, [data]);

  // Mutation to update transcription method
  const mutation = useMutation({
    mutationFn: async (method: string) => {
      const response = await fetch('/api/config/transcription-method', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ method }),
      });
      
      if (!response.ok) {
        throw new Error('Failed to update transcription method');
      }
      
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/config/transcription-method'] });
      toast({
        title: 'Settings Updated',
        description: `Transcription method has been changed to ${selectedMethod}`,
      });
    },
    onError: (error) => {
      toast({
        title: 'Error',
        description: `Failed to update transcription method: ${error}`,
        variant: 'destructive',
      });
    },
  });

  const handleSave = () => {
    if (selectedMethod && selectedMethod !== data?.current_method) {
      mutation.mutate(selectedMethod);
    }
  };

  if (isLoading) {
    return (
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Transcription Settings</CardTitle>
          <CardDescription>Configure how audio files are transcribed</CardDescription>
        </CardHeader>
        <CardContent className="flex justify-center py-6">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Transcription Settings</CardTitle>
          <CardDescription>Could not load settings</CardDescription>
        </CardHeader>
        <CardContent>
          <p className="text-destructive">Error loading transcription settings</p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="w-full">
      <CardHeader>
        <CardTitle>Transcription Settings</CardTitle>
        <CardDescription>Configure how audio files are transcribed</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-6">
          <div className="space-y-2">
            <h3 className="text-sm font-medium">Transcription Method</h3>
            <p className="text-sm text-muted-foreground">
              Choose which method to use for transcribing audio files with Deepgram
            </p>
            <Select value={selectedMethod} onValueChange={setSelectedMethod}>
              <SelectTrigger className="w-full md:w-[240px]">
                <SelectValue placeholder="Select transcription method" />
              </SelectTrigger>
              <SelectContent>
                {data?.available_methods.map((method) => (
                  <SelectItem key={method} value={method}>
                    {method === 'sdk' ? 'Deepgram SDK' : method === 'rest_api' ? 'REST API' : 'Direct REST API'}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <div className="mt-3 p-3 bg-muted/30 rounded-md">
              {selectedMethod === 'sdk' ? (
                <div className="space-y-2">
                  <h4 className="text-sm font-medium">Deepgram SDK</h4>
                  <p className="text-xs text-muted-foreground">
                    Uses the official Deepgram SDK for transcription with better error handling and direct authentication.
                  </p>
                  <div className="flex gap-2 text-xs">
                    <span className="px-2 py-1 bg-green-100 text-green-800 rounded-full">Reliable</span>
                    <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded-full">Official Support</span>
                    <span className="px-2 py-1 bg-amber-100 text-amber-800 rounded-full">Higher Latency</span>
                  </div>
                </div>
              ) : selectedMethod === 'rest_api' ? (
                <div className="space-y-2">
                  <h4 className="text-sm font-medium">REST API</h4>
                  <p className="text-xs text-muted-foreground">
                    Uses direct REST API calls to Deepgram (original implementation).
                  </p>
                  <div className="flex gap-2 text-xs">
                    <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded-full">Standard</span>
                    <span className="px-2 py-1 bg-amber-100 text-amber-800 rounded-full">Medium Performance</span>
                  </div>
                </div>
              ) : (
                <div className="space-y-2">
                  <h4 className="text-sm font-medium">Direct REST API</h4>
                  <p className="text-xs text-muted-foreground">
                    Uses the optimized REST API implementation with proven reliability for Azure blob transcription.
                  </p>
                  <div className="flex gap-2 text-xs">
                    <span className="px-2 py-1 bg-green-100 text-green-800 rounded-full">Optimized</span>
                    <span className="px-2 py-1 bg-green-100 text-green-800 rounded-full">Faster</span>
                    <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded-full">Azure Integration</span>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </CardContent>
      <CardFooter className="justify-end">
        <Button 
          onClick={handleSave} 
          disabled={mutation.isPending || selectedMethod === data?.current_method}
        >
          {mutation.isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
          Save Changes
        </Button>
      </CardFooter>
    </Card>
  );
}