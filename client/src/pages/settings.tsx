import React from 'react';
import { SettingsPanel } from '@/components/settings-panel';

export default function SettingsPage() {
  return (
    <div className="container mx-auto py-8">
      <div className="flex flex-col gap-8">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Settings</h1>
          <p className="text-muted-foreground mt-2">
            Configure Deepgram transcription settings and system preferences
          </p>
        </div>
        
        <div className="grid gap-8">
          <SettingsPanel />
          
          {/* Add more settings panels here as needed */}
        </div>
      </div>
    </div>
  );
}