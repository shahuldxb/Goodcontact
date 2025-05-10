import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { getRandomColor } from "@/lib/utils";
import { ChevronRight } from "lucide-react";

interface AnalyticsCardsProps {
  sentimentData?: {
    positive: number;
    neutral: number;
    negative: number;
  };
  topicsData?: Array<{
    name: string;
    percentage: number;
  }>;
  sentimentLoading: boolean;
  topicsLoading: boolean;
}

export default function AnalyticsCards({
  sentimentData,
  topicsData,
  sentimentLoading,
  topicsLoading
}: AnalyticsCardsProps) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
      {/* Sentiment Analysis Card */}
      <Card className="bg-white hover:shadow-md transition-shadow">
        <CardContent className="p-5">
          <div className="flex justify-between items-center mb-4">
            <h3 className="text-lg font-semibold text-gray-800">Sentiment Analysis</h3>
            <Button variant="link" size="sm" className="text-primary">
              View All <ChevronRight className="h-4 w-4 ml-1" />
            </Button>
          </div>
          
          {/* Visualization placeholder */}
          <div className="h-48 rounded-lg w-full mb-4 bg-gray-100 overflow-hidden">
            <img 
              src="https://images.unsplash.com/photo-1551288049-bebda4e38f71?ixlib=rb-4.0.3&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=800&h=400"
              alt="Data visualization chart" 
              className="w-full h-full object-cover"
            />
          </div>
          
          <div className="grid grid-cols-3 gap-4 mt-4">
            {sentimentLoading ? (
              <>
                <div className="text-center">
                  <Skeleton className="h-8 w-16 mx-auto mb-1" />
                  <Skeleton className="h-4 w-12 mx-auto" />
                </div>
                <div className="text-center">
                  <Skeleton className="h-8 w-16 mx-auto mb-1" />
                  <Skeleton className="h-4 w-12 mx-auto" />
                </div>
                <div className="text-center">
                  <Skeleton className="h-8 w-16 mx-auto mb-1" />
                  <Skeleton className="h-4 w-12 mx-auto" />
                </div>
              </>
            ) : (
              <>
                <div className="text-center">
                  <div className="text-success font-semibold text-xl">{sentimentData?.positive || 0}%</div>
                  <div className="text-xs text-gray-500">Positive</div>
                </div>
                <div className="text-center">
                  <div className="text-warning font-semibold text-xl">{sentimentData?.neutral || 0}%</div>
                  <div className="text-xs text-gray-500">Neutral</div>
                </div>
                <div className="text-center">
                  <div className="text-danger font-semibold text-xl">{sentimentData?.negative || 0}%</div>
                  <div className="text-xs text-gray-500">Negative</div>
                </div>
              </>
            )}
          </div>
        </CardContent>
      </Card>
      
      {/* Topic Detection Card */}
      <Card className="bg-white hover:shadow-md transition-shadow">
        <CardContent className="p-5">
          <div className="flex justify-between items-center mb-4">
            <h3 className="text-lg font-semibold text-gray-800">Topic Detection</h3>
            <Button variant="link" size="sm" className="text-primary">
              View All <ChevronRight className="h-4 w-4 ml-1" />
            </Button>
          </div>
          
          <div className="space-y-3">
            {topicsLoading ? (
              <>
                <div className="flex items-center">
                  <div className="w-full mr-4">
                    <div className="flex justify-between mb-1">
                      <Skeleton className="h-4 w-32" />
                      <Skeleton className="h-4 w-8" />
                    </div>
                    <Skeleton className="h-2 w-full" />
                  </div>
                </div>
                <div className="flex items-center">
                  <div className="w-full mr-4">
                    <div className="flex justify-between mb-1">
                      <Skeleton className="h-4 w-32" />
                      <Skeleton className="h-4 w-8" />
                    </div>
                    <Skeleton className="h-2 w-full" />
                  </div>
                </div>
                <div className="flex items-center">
                  <div className="w-full mr-4">
                    <div className="flex justify-between mb-1">
                      <Skeleton className="h-4 w-32" />
                      <Skeleton className="h-4 w-8" />
                    </div>
                    <Skeleton className="h-2 w-full" />
                  </div>
                </div>
              </>
            ) : topicsData && topicsData.length > 0 ? (
              topicsData.map((topic, index) => (
                <div key={topic.name} className="flex items-center">
                  <div className="w-full mr-4">
                    <div className="flex justify-between mb-1">
                      <span className="text-sm font-medium text-gray-700">{topic.name}</span>
                      <span className="text-sm font-medium text-gray-700">{topic.percentage}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className={`${getRandomColor(index)} h-2 rounded-full`} 
                        style={{width: `${topic.percentage}%`}}
                      ></div>
                    </div>
                  </div>
                </div>
              ))
            ) : (
              <div className="text-center py-4 text-gray-500">
                No topic data available
              </div>
            )}
          </div>
        </CardContent>
      </Card>
      
      {/* Recent Calls with Issues */}
      <Card className="bg-white hover:shadow-md transition-shadow md:col-span-2">
        <CardContent className="p-5">
          <div className="flex justify-between items-center mb-4">
            <h3 className="text-lg font-semibold text-gray-800">Recently Processed Files</h3>
            <Button variant="link" size="sm" className="text-primary">
              View All <ChevronRight className="h-4 w-4 ml-1" />
            </Button>
          </div>
          
          <div className="overflow-x-auto">
            <table className="min-w-full bg-white">
              <thead>
                <tr className="bg-gray-100 text-gray-700 text-sm">
                  <th className="py-3 px-4 text-left">File Name</th>
                  <th className="py-3 px-4 text-left">Processing Date</th>
                  <th className="py-3 px-4 text-left">Language</th>
                  <th className="py-3 px-4 text-left">Duration</th>
                  <th className="py-3 px-4 text-left">Sentiment</th>
                  <th className="py-3 px-4 text-left">Actions</th>
                </tr>
              </thead>
              <tbody>
                <tr className="border-b border-gray-200 hover:bg-gray-50">
                  <td className="py-3 px-4 font-medium">call_recording_001.wav</td>
                  <td className="py-3 px-4">2023-06-02 14:23</td>
                  <td className="py-3 px-4">English</td>
                  <td className="py-3 px-4">3:42</td>
                  <td className="py-3 px-4">
                    <span className="bg-green-100 text-green-800 text-xs font-medium px-2.5 py-0.5 rounded">Positive</span>
                  </td>
                  <td className="py-3 px-4">
                    <div className="flex space-x-2">
                      <Button variant="ghost" size="sm" className="text-primary h-7 w-7 p-0">
                        <i className="bi bi-eye"></i>
                      </Button>
                      <Button variant="ghost" size="sm" className="text-gray-600 h-7 w-7 p-0">
                        <i className="bi bi-download"></i>
                      </Button>
                      <Button variant="ghost" size="sm" className="text-gray-600 h-7 w-7 p-0">
                        <i className="bi bi-three-dots-vertical"></i>
                      </Button>
                    </div>
                  </td>
                </tr>
                <tr className="border-b border-gray-200 hover:bg-gray-50">
                  <td className="py-3 px-4 font-medium">support_call_002.wav</td>
                  <td className="py-3 px-4">2023-06-01 11:15</td>
                  <td className="py-3 px-4">English</td>
                  <td className="py-3 px-4">5:12</td>
                  <td className="py-3 px-4">
                    <span className="bg-yellow-100 text-yellow-800 text-xs font-medium px-2.5 py-0.5 rounded">Neutral</span>
                  </td>
                  <td className="py-3 px-4">
                    <div className="flex space-x-2">
                      <Button variant="ghost" size="sm" className="text-primary h-7 w-7 p-0">
                        <i className="bi bi-eye"></i>
                      </Button>
                      <Button variant="ghost" size="sm" className="text-gray-600 h-7 w-7 p-0">
                        <i className="bi bi-download"></i>
                      </Button>
                      <Button variant="ghost" size="sm" className="text-gray-600 h-7 w-7 p-0">
                        <i className="bi bi-three-dots-vertical"></i>
                      </Button>
                    </div>
                  </td>
                </tr>
                <tr className="border-b border-gray-200 hover:bg-gray-50">
                  <td className="py-3 px-4 font-medium">customer_complaint_003.wav</td>
                  <td className="py-3 px-4">2023-05-28 09:45</td>
                  <td className="py-3 px-4">English</td>
                  <td className="py-3 px-4">4:37</td>
                  <td className="py-3 px-4">
                    <span className="bg-red-100 text-red-800 text-xs font-medium px-2.5 py-0.5 rounded">Negative</span>
                  </td>
                  <td className="py-3 px-4">
                    <div className="flex space-x-2">
                      <Button variant="ghost" size="sm" className="text-primary h-7 w-7 p-0">
                        <i className="bi bi-eye"></i>
                      </Button>
                      <Button variant="ghost" size="sm" className="text-gray-600 h-7 w-7 p-0">
                        <i className="bi bi-download"></i>
                      </Button>
                      <Button variant="ghost" size="sm" className="text-gray-600 h-7 w-7 p-0">
                        <i className="bi bi-three-dots-vertical"></i>
                      </Button>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
