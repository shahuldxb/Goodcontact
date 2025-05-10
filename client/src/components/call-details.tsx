import { useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Tabs, TabsContent } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { getSentimentBgColor, getRandomColor } from "@/lib/utils";

interface CallDetailsProps {
  data: any;
  activeTab: string;
}

export default function CallDetails({ data, activeTab }: CallDetailsProps) {
  const [speakerColors] = useState<Record<string, string>>({
    0: 'text-primary',
    1: 'text-secondary',
    2: 'text-success',
    3: 'text-warning',
    4: 'text-danger',
  });

  if (!data) {
    return (
      <Card className="bg-white">
        <CardContent className="p-5 text-center py-8">
          <h3 className="text-lg font-semibold text-gray-800 mb-2">No Data Available</h3>
          <p className="text-gray-600">Analysis data could not be loaded for this file.</p>
        </CardContent>
      </Card>
    );
  }

  const asset = data.asset || {};
  const sentiment = data.sentiment || {};
  const language = data.language || {};
  const summarization = data.summarization || {};
  const forbiddenPhrases = data.forbiddenPhrases || {};
  const topicModeling = data.topicModeling || {};
  const speakerDiarization = data.speakerDiarization || {};

  return (
    <>
      <Card className="bg-white mb-4">
        <CardContent className="p-5">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
            <div className="p-4 bg-gray-50 rounded-lg">
              <p className="text-sm text-gray-500">File Information</p>
              <ul className="mt-2 space-y-1 text-sm">
                <li><span className="font-medium">Filename:</span> {asset.filename || "N/A"}</li>
                <li><span className="font-medium">Size:</span> {asset.fileSize ? Math.round(asset.fileSize / 1024 / 1024 * 10) / 10 + ' MB' : "N/A"}</li>
                <li><span className="font-medium">Uploaded:</span> {asset.uploadDate ? new Date(asset.uploadDate).toLocaleDateString() : "N/A"}</li>
                <li><span className="font-medium">Processed:</span> {asset.processedDate ? new Date(asset.processedDate).toLocaleDateString() : "N/A"}</li>
              </ul>
            </div>
            
            <div className="p-4 bg-gray-50 rounded-lg">
              <p className="text-sm text-gray-500">Analysis Summary</p>
              <ul className="mt-2 space-y-1 text-sm">
                <li>
                  <span className="font-medium">Sentiment:</span>{' '}
                  <span className={sentiment.overallSentiment && getSentimentBgColor(sentiment.overallSentiment).split(' ')[0]}>
                    {sentiment.overallSentiment || "N/A"}
                  </span>
                </li>
                <li><span className="font-medium">Language:</span> {language.language || asset.languageDetected || "English"}</li>
                <li><span className="font-medium">Speakers:</span> {speakerDiarization.speakerCount || "1"}</li>
                <li>
                  <span className="font-medium">Risk Level:</span>{' '}
                  {forbiddenPhrases.riskLevel || "Low"}
                </li>
              </ul>
            </div>
            
            <div className="p-4 bg-gray-50 rounded-lg">
              <p className="text-sm text-gray-500">Call Summary</p>
              <p className="mt-2 text-sm">
                {summarization.summary || 
                 "Call summary is not available for this recording. Please process the file using the Deepgram summarization feature to view a summary."}
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Tabs value={activeTab}>
        <TabsContent value="transcription" className="mt-0">
          <Card className="bg-white">
            <CardContent className="p-5">
              <h4 className="font-medium text-gray-800 mb-2">Transcription</h4>
              <div className="bg-gray-50 p-4 rounded-lg max-h-96 overflow-y-auto">
                {asset.transcription ? (
                  <p className="whitespace-pre-line">{asset.transcription}</p>
                ) : (
                  <p className="text-gray-500 text-center py-4">No transcription available for this recording.</p>
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="sentiment" className="mt-0">
          <Card className="bg-white">
            <CardContent className="p-5">
              <h4 className="font-medium text-gray-800 mb-2">Sentiment Analysis</h4>
              
              {sentiment.overallSentiment ? (
                <>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                    <div className="p-4 bg-gray-50 rounded-lg text-center">
                      <p className="text-sm text-gray-500 mb-1">Overall Sentiment</p>
                      <Badge className={getSentimentBgColor(sentiment.overallSentiment)}>
                        {sentiment.overallSentiment}
                      </Badge>
                    </div>
                    <div className="p-4 bg-gray-50 rounded-lg text-center">
                      <p className="text-sm text-gray-500 mb-1">Confidence Score</p>
                      <span className="text-lg font-semibold">
                        {sentiment.confidenceScore ? `${sentiment.confidenceScore}%` : "N/A"}
                      </span>
                    </div>
                    <div className="p-4 bg-gray-50 rounded-lg text-center">
                      <p className="text-sm text-gray-500 mb-1">Sentiment Shifts</p>
                      <span className="text-lg font-semibold">
                        {sentiment.sentimentBySegment ? sentiment.sentimentBySegment.length : "0"}
                      </span>
                    </div>
                  </div>
                  
                  {sentiment.sentimentBySegment && sentiment.sentimentBySegment.length > 0 && (
                    <div>
                      <h5 className="font-medium text-gray-700 mb-2">Sentiment by Segment</h5>
                      <div className="space-y-3">
                        {sentiment.sentimentBySegment.map((segment: any, index: number) => (
                          <div key={index} className="p-3 bg-gray-50 rounded-lg">
                            <div className="flex justify-between mb-1">
                              <Badge className={getSentimentBgColor(segment.sentiment)}>
                                {segment.sentiment}
                              </Badge>
                              <span className="text-sm text-gray-500">
                                Confidence: {Math.round(segment.confidence * 100)}%
                              </span>
                            </div>
                            <p className="text-sm text-gray-700">{segment.text}</p>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </>
              ) : (
                <div className="text-gray-500 text-center py-4">
                  No sentiment analysis data available for this recording.
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="topics" className="mt-0">
          <Card className="bg-white">
            <CardContent className="p-5">
              <h4 className="font-medium text-gray-800 mb-2">Topic Modeling</h4>
              
              {topicModeling.topicsDetected ? (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <h5 className="font-medium text-gray-700 mb-2">Detected Topics</h5>
                    <div className="bg-gray-50 p-4 rounded-lg">
                      <ul className="space-y-2">
                        {Array.isArray(topicModeling.topicsDetected) ? 
                          topicModeling.topicsDetected.map((topic: any, index: number) => (
                            <li key={index} className="flex items-center">
                              <span className={`${getRandomColor(index)} h-3 w-3 rounded-full mr-2`}></span>
                              <span className="text-sm">
                                {topic.keywords?.join(", ") || `Topic ${index + 1}`} ({Math.round(topic.score * 100)}%)
                              </span>
                            </li>
                          )) : (
                            <li className="text-gray-500">Topic data format is not valid</li>
                          )
                        }
                      </ul>
                    </div>
                  </div>
                  
                  <div>
                    <h5 className="font-medium text-gray-700 mb-2">Call Context Analysis</h5>
                    <div className="bg-gray-50 p-4 rounded-lg h-full">
                      <p className="text-sm text-gray-700">
                        Based on the topic analysis, this call appears to be primarily about 
                        {topicModeling.topicsDetected[0]?.keywords?.join(", ") || "various topics"}. 
                        The call discusses {topicModeling.topicsDetected.length || 0} distinct topics, 
                        which may indicate a {topicModeling.topicsDetected.length > 2 ? "complex" : "focused"} conversation.
                      </p>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="text-gray-500 text-center py-4">
                  No topic modeling data available for this recording.
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="speakers" className="mt-0">
          <Card className="bg-white">
            <CardContent className="p-5">
              <h4 className="font-medium text-gray-800 mb-2">Speaker Diarization</h4>
              
              {speakerDiarization.speakerCount ? (
                <>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                    <div className="p-4 bg-gray-50 rounded-lg text-center">
                      <p className="text-sm text-gray-500 mb-1">Number of Speakers</p>
                      <span className="text-lg font-semibold">{speakerDiarization.speakerCount}</span>
                    </div>
                    
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <p className="text-sm text-gray-500 mb-1">Talk Time Distribution</p>
                      {speakerDiarization.speakerMetrics?.speakerTalkTime && (
                        <div className="space-y-2 mt-2">
                          {Object.entries(speakerDiarization.speakerMetrics.speakerTalkTime).map(([speaker, time]: [string, any]) => (
                            <div key={speaker} className="flex items-center">
                              <div className="w-full">
                                <div className="flex justify-between mb-1">
                                  <span className={`text-xs ${speakerColors[speaker] || 'text-gray-700'}`}>
                                    Speaker {speaker}
                                  </span>
                                  <span className="text-xs text-gray-600">
                                    {Math.round(time)} sec
                                  </span>
                                </div>
                                <div className="w-full bg-gray-200 rounded-full h-1.5">
                                  <div 
                                    className={`${getRandomColor(parseInt(speaker))} h-1.5 rounded-full`} 
                                    style={{
                                      width: `${Math.round((time / 
                                        Object.values(speakerDiarization.speakerMetrics.speakerTalkTime).reduce((a: number, b: number) => a + b, 0)
                                      ) * 100)}%`
                                    }}
                                  ></div>
                                </div>
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                    
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <p className="text-sm text-gray-500 mb-1">Word Count Distribution</p>
                      {speakerDiarization.speakerMetrics?.speakerWordCount && (
                        <div className="space-y-2 mt-2">
                          {Object.entries(speakerDiarization.speakerMetrics.speakerWordCount).map(([speaker, count]: [string, any]) => (
                            <div key={speaker} className="flex items-center">
                              <div className="w-full">
                                <div className="flex justify-between mb-1">
                                  <span className={`text-xs ${speakerColors[speaker] || 'text-gray-700'}`}>
                                    Speaker {speaker}
                                  </span>
                                  <span className="text-xs text-gray-600">
                                    {count} words
                                  </span>
                                </div>
                                <div className="w-full bg-gray-200 rounded-full h-1.5">
                                  <div 
                                    className={`${getRandomColor(parseInt(speaker))} h-1.5 rounded-full`} 
                                    style={{
                                      width: `${Math.round((count / 
                                        Object.values(speakerDiarization.speakerMetrics.speakerWordCount).reduce((a: number, b: number) => a + b, 0)
                                      ) * 100)}%`
                                    }}
                                  ></div>
                                </div>
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                  
                  <h5 className="font-medium text-gray-700 mb-2">Speaker Segments</h5>
                  <div className="bg-gray-50 p-4 rounded-lg max-h-64 overflow-y-auto">
                    {data.speakerSegments && data.speakerSegments.length > 0 ? (
                      data.speakerSegments.map((segment: any, index: number) => (
                        <div key={index} className="mb-2">
                          <span className={`font-medium ${speakerColors[segment.speakerId] || 'text-gray-800'}`}>
                            Speaker {segment.speakerId} ({segment.startTime}s - {segment.endTime}s):
                          </span>{' '}
                          <span>{segment.text}</span>
                        </div>
                      ))
                    ) : (
                      <p className="text-center text-gray-500">No speaker segments available</p>
                    )}
                  </div>
                </>
              ) : (
                <div className="text-gray-500 text-center py-4">
                  No speaker diarization data available for this recording.
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="forbidden" className="mt-0">
          <Card className="bg-white">
            <CardContent className="p-5">
              <h4 className="font-medium text-gray-800 mb-2">Forbidden Phrases Analysis</h4>
              
              {forbiddenPhrases.riskLevel ? (
                <>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                    <div className="p-4 bg-gray-50 rounded-lg text-center">
                      <p className="text-sm text-gray-500 mb-1">Risk Level</p>
                      <Badge className={
                        forbiddenPhrases.riskLevel === 'High' ? 'bg-red-100 text-red-800' :
                        forbiddenPhrases.riskLevel === 'Medium' ? 'bg-yellow-100 text-yellow-800' :
                        'bg-green-100 text-green-800'
                      }>
                        {forbiddenPhrases.riskLevel}
                      </Badge>
                    </div>
                    
                    <div className="p-4 bg-gray-50 rounded-lg text-center">
                      <p className="text-sm text-gray-500 mb-1">Risk Score</p>
                      <span className="text-lg font-semibold">
                        {forbiddenPhrases.riskScore ? `${forbiddenPhrases.riskScore}/100` : "N/A"}
                      </span>
                    </div>
                    
                    <div className="p-4 bg-gray-50 rounded-lg text-center">
                      <p className="text-sm text-gray-500 mb-1">Categories Detected</p>
                      <span className="text-lg font-semibold">
                        {forbiddenPhrases.categoriesDetected ? 
                          Object.keys(forbiddenPhrases.categoriesDetected).length : "0"}
                      </span>
                    </div>
                  </div>
                  
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <h5 className="font-medium text-gray-700 mb-2">Categories Breakdown</h5>
                      <div className="bg-gray-50 p-4 rounded-lg">
                        {forbiddenPhrases.categoriesDetected ? (
                          <ul className="space-y-2">
                            {Object.entries(forbiddenPhrases.categoriesDetected).map(([category, count]: [string, any]) => (
                              <li key={category} className="flex items-center justify-between">
                                <span className="text-sm capitalize">{category.replace(/_/g, ' ')}</span>
                                <Badge variant="outline" className="bg-gray-200">
                                  {count} occurrences
                                </Badge>
                              </li>
                            ))}
                          </ul>
                        ) : (
                          <p className="text-center text-gray-500">No categories detected</p>
                        )}
                      </div>
                    </div>
                    
                    <div>
                      <h5 className="font-medium text-gray-700 mb-2">Flagged Phrases</h5>
                      <div className="bg-gray-50 p-4 rounded-lg">
                        {data.forbiddenPhrasesDetails && data.forbiddenPhrasesDetails.length > 0 ? (
                          <ul className="space-y-2 text-sm">
                            {data.forbiddenPhrasesDetails.map((detail: any, index: number) => (
                              <li key={index} className={
                                detail.category === 'financial_promises' || detail.category === 'discriminatory_language' ? 
                                'text-danger' : 'text-warning'
                              }>
                                "{detail.phrase}" ({detail.startTime}s) - {detail.category.replace(/_/g, ' ')}
                              </li>
                            ))}
                          </ul>
                        ) : (
                          <p className="text-center text-gray-500">No flagged phrases detected</p>
                        )}
                      </div>
                    </div>
                  </div>
                </>
              ) : (
                <div className="text-gray-500 text-center py-4">
                  No forbidden phrases analysis data available for this recording.
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </>
  );
}
