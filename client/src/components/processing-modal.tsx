import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";

interface ProcessingModalProps {
  isOpen: boolean;
  onClose: () => void;
  progress: {
    transcription: number;
    sentimentAnalysis: number;
    languageDetection: number;
    summarization: number;
    forbiddenPhrases: number;
    topicModeling: number;
    speakerDiarization: number;
  };
}

export default function ProcessingModal({ isOpen, onClose, progress }: ProcessingModalProps) {
  const isProcessingComplete = 
    progress.transcription === 100 && 
    progress.sentimentAnalysis === 100 && 
    progress.languageDetection === 100 && 
    progress.summarization === 100 && 
    progress.forbiddenPhrases === 100 && 
    progress.topicModeling === 100 && 
    progress.speakerDiarization === 100;

  const getStatus = (percent: number) => {
    if (percent === 100) return <span className="text-sm font-medium text-green-600">Completed</span>;
    if (percent > 0) return <span className="text-sm font-medium text-primary">In Progress</span>;
    return <span className="text-sm font-medium text-gray-500">Pending</span>;
  };

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Processing Files</DialogTitle>
        </DialogHeader>
        
        <div className="py-4">
          <p className="text-gray-600 mb-6">Deepgram is analyzing your files. This may take a few moments.</p>
          
          <div className="space-y-4">
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium text-gray-700">Transcription</span>
                {getStatus(progress.transcription)}
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className={`${progress.transcription === 100 ? 'bg-green-600' : 'bg-primary'} h-2 rounded-full`} 
                  style={{width: `${progress.transcription}%`}}
                ></div>
              </div>
            </div>
            
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium text-gray-700">Sentiment Analysis</span>
                {getStatus(progress.sentimentAnalysis)}
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className={`${progress.sentimentAnalysis === 100 ? 'bg-green-600' : 'bg-primary'} h-2 rounded-full`} 
                  style={{width: `${progress.sentimentAnalysis}%`}}
                ></div>
              </div>
            </div>
            
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium text-gray-700">Language Detection</span>
                {getStatus(progress.languageDetection)}
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className={`${progress.languageDetection === 100 ? 'bg-green-600' : 'bg-primary'} h-2 rounded-full`} 
                  style={{width: `${progress.languageDetection}%`}}
                ></div>
              </div>
            </div>
            
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium text-gray-700">Call Summarization</span>
                {getStatus(progress.summarization)}
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className={`${progress.summarization === 100 ? 'bg-green-600' : 'bg-primary'} h-2 rounded-full`} 
                  style={{width: `${progress.summarization}%`}}
                ></div>
              </div>
            </div>
            
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium text-gray-700">Forbidden Phrases</span>
                {getStatus(progress.forbiddenPhrases)}
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className={`${progress.forbiddenPhrases === 100 ? 'bg-green-600' : 'bg-primary'} h-2 rounded-full`} 
                  style={{width: `${progress.forbiddenPhrases}%`}}
                ></div>
              </div>
            </div>
            
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium text-gray-700">Topic Modeling</span>
                {getStatus(progress.topicModeling)}
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className={`${progress.topicModeling === 100 ? 'bg-green-600' : 'bg-primary'} h-2 rounded-full`} 
                  style={{width: `${progress.topicModeling}%`}}
                ></div>
              </div>
            </div>
            
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium text-gray-700">Speaker Diarization</span>
                {getStatus(progress.speakerDiarization)}
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className={`${progress.speakerDiarization === 100 ? 'bg-green-600' : 'bg-primary'} h-2 rounded-full`} 
                  style={{width: `${progress.speakerDiarization}%`}}
                ></div>
              </div>
            </div>
          </div>
        </div>
        
        <DialogFooter>
          <Button
            variant="outline"
            onClick={onClose}
          >
            Cancel
          </Button>
          <Button
            disabled={!isProcessingComplete}
            onClick={onClose}
          >
            {isProcessingComplete ? 'View Results' : 'Processing...'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
