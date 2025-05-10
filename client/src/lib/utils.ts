import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatBytes(bytes: number, decimals = 2): string {
  if (bytes === 0) return "0 Bytes";
  
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB"];
  
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + " " + sizes[i];
}

export function formatDate(date: Date | string): string {
  if (!date) return 'N/A';
  const d = new Date(date);
  return d.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric'
  });
}

export function formatTime(seconds: number): string {
  if (!seconds && seconds !== 0) return 'N/A';
  
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = Math.floor(seconds % 60);
  
  return `${minutes}:${remainingSeconds.toString().padStart(2, '0')}`;
}

export function truncateText(text: string, maxLength = 100): string {
  if (!text) return '';
  return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
}

export function getFileTypeLabel(filename: string): string {
  const extension = filename.split('.').pop()?.toLowerCase();
  
  const typeMap: Record<string, string> = {
    'wav': 'WAV',
    'mp3': 'MP3',
    'ogg': 'OGG',
    'm4a': 'M4A',
    'flac': 'FLAC'
  };
  
  return typeMap[extension || ''] || 'Audio';
}

export function getSentimentColor(sentiment: string): string {
  const sentimentMap: Record<string, string> = {
    'positive': 'text-success',
    'neutral': 'text-warning',
    'negative': 'text-danger'
  };
  
  return sentimentMap[sentiment.toLowerCase()] || 'text-gray-600';
}

export function getSentimentBgColor(sentiment: string): string {
  const sentimentMap: Record<string, string> = {
    'positive': 'bg-green-100 text-green-800',
    'neutral': 'bg-yellow-100 text-yellow-800',
    'negative': 'bg-red-100 text-red-800'
  };
  
  return sentimentMap[sentiment.toLowerCase()] || 'bg-gray-100 text-gray-800';
}

export function getRandomColor(index: number) {
  const colors = [
    'bg-primary',
    'bg-secondary',
    'bg-success',
    'bg-warning',
    'bg-danger',
  ];
  return colors[index % colors.length];
}

export function calculatePercentChange(current: number, previous: number): {value: number, isPositive: boolean} {
  if (previous === 0) return { value: 0, isPositive: false };
  
  const change = ((current - previous) / previous) * 100;
  return { 
    value: Math.abs(parseFloat(change.toFixed(1))), 
    isPositive: change >= 0 
  };
}
