import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { 
  FileText, 
  CheckCircle, 
  Timer, 
  AlertTriangle 
} from "lucide-react";

interface StatisticsCardsProps {
  isLoading: boolean;
  stats?: {
    totalFiles: number;
    processedCount: number;
    processingTime: number;
    flaggedCalls: number;
  };
}

export default function StatisticsCards({ isLoading, stats }: StatisticsCardsProps) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
      {/* Total Files Card */}
      <Card className="bg-white hover:shadow-md transition-shadow">
        <CardContent className="p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 mb-1">Total Files</p>
              {isLoading ? (
                <Skeleton className="h-8 w-16" />
              ) : (
                <h3 className="text-2xl font-semibold text-gray-800">{stats?.totalFiles || 0}</h3>
              )}
              <p className="text-xs text-success mt-2 flex items-center">
                <i className="bi bi-arrow-up mr-1"></i>
                <span>+12.4% from last month</span>
              </p>
            </div>
            <div className="bg-primary bg-opacity-10 p-3 rounded-lg">
              <FileText className="text-primary h-6 w-6" />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Processed Files Card */}
      <Card className="bg-white hover:shadow-md transition-shadow">
        <CardContent className="p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 mb-1">Processed Files</p>
              {isLoading ? (
                <Skeleton className="h-8 w-16" />
              ) : (
                <h3 className="text-2xl font-semibold text-gray-800">{stats?.processedCount || 0}</h3>
              )}
              <p className="text-xs text-success mt-2 flex items-center">
                <i className="bi bi-arrow-up mr-1"></i>
                <span>+8.2% from last month</span>
              </p>
            </div>
            <div className="bg-success bg-opacity-10 p-3 rounded-lg">
              <CheckCircle className="text-success h-6 w-6" />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Processing Time Card */}
      <Card className="bg-white hover:shadow-md transition-shadow">
        <CardContent className="p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 mb-1">Processing Time</p>
              {isLoading ? (
                <Skeleton className="h-8 w-16" />
              ) : (
                <h3 className="text-2xl font-semibold text-gray-800">{stats?.processingTime || 0}s</h3>
              )}
              <p className="text-xs text-danger mt-2 flex items-center">
                <i className="bi bi-arrow-up mr-1"></i>
                <span>+0.8s from last month</span>
              </p>
            </div>
            <div className="bg-warning bg-opacity-10 p-3 rounded-lg">
              <Timer className="text-warning h-6 w-6" />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Flagged Calls Card */}
      <Card className="bg-white hover:shadow-md transition-shadow">
        <CardContent className="p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 mb-1">Flagged Calls</p>
              {isLoading ? (
                <Skeleton className="h-8 w-16" />
              ) : (
                <h3 className="text-2xl font-semibold text-gray-800">{stats?.flaggedCalls || 0}</h3>
              )}
              <p className="text-xs text-danger mt-2 flex items-center">
                <i className="bi bi-arrow-up mr-1"></i>
                <span>+4 from last month</span>
              </p>
            </div>
            <div className="bg-danger bg-opacity-10 p-3 rounded-lg">
              <AlertTriangle className="text-danger h-6 w-6" />
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
