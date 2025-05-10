import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { 
  PlayCircle, 
  Download, 
  MoreVertical, 
  Search
} from "lucide-react";
import { formatBytes, formatDate, getFileTypeLabel } from "@/lib/utils";

interface SourceFilesGridProps {
  onSelectFiles: (files: string[]) => void;
  selectedFiles: string[];
  onProcessFiles: () => void;
}

export default function SourceFilesGrid({ onSelectFiles, selectedFiles, onProcessFiles }: SourceFilesGridProps) {
  const [searchTerm, setSearchTerm] = useState<string>("");
  const [selectAll, setSelectAll] = useState<boolean>(false);
  const [currentPage, setCurrentPage] = useState<number>(1);
  const itemsPerPage = 5;

  // Fetch source files
  const { data, isLoading, error } = useQuery({
    queryKey: ['/api/files/source'],
    staleTime: 30000, // 30 seconds
  });

  // Filter files based on search term
  const filteredFiles = data?.files?.filter((file: any) => 
    file.name.toLowerCase().includes(searchTerm.toLowerCase())
  ) || [];

  // Pagination
  const totalPages = Math.ceil(filteredFiles.length / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const paginatedFiles = filteredFiles.slice(startIndex, startIndex + itemsPerPage);

  // Handle select all checkbox
  useEffect(() => {
    if (selectAll) {
      onSelectFiles(filteredFiles.map((file: any) => file.name));
    } else if (selectedFiles.length === filteredFiles.length) {
      onSelectFiles([]);
    }
  }, [selectAll]);

  // Update selectAll state when individual selections change
  useEffect(() => {
    setSelectAll(selectedFiles.length === filteredFiles.length && filteredFiles.length > 0);
  }, [selectedFiles, filteredFiles]);

  // Handle individual file selection
  const handleFileSelect = (fileName: string, isSelected: boolean) => {
    if (isSelected) {
      onSelectFiles([...selectedFiles, fileName]);
    } else {
      onSelectFiles(selectedFiles.filter(name => name !== fileName));
    }
  };

  return (
    <Card className="bg-white">
      <CardContent className="p-5">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-lg font-semibold text-gray-800">Source Files (shahulin container)</h3>
          <div className="flex items-center space-x-2">
            <div className="relative">
              <Search className="absolute left-3 top-2.5 h-4 w-4 text-gray-400" />
              <Input
                type="text"
                placeholder="Search files..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-9"
              />
            </div>
            <Button 
              disabled={selectedFiles.length === 0} 
              onClick={onProcessFiles}
              className="bg-primary text-white"
            >
              <PlayCircle className="h-4 w-4 mr-1" />
              Process Selected
            </Button>
          </div>
        </div>
        
        <div className="overflow-x-auto">
          {isLoading ? (
            <div className="py-8 text-center">
              <div className="inline-block h-8 w-8 animate-spin rounded-full border-4 border-solid border-primary border-r-transparent" />
              <p className="mt-2 text-gray-600">Loading files...</p>
            </div>
          ) : error ? (
            <div className="py-8 text-center">
              <p className="text-danger">Error loading files. Please try again.</p>
            </div>
          ) : filteredFiles.length === 0 ? (
            <div className="py-8 text-center">
              <p className="text-gray-600">No files found in source container.</p>
            </div>
          ) : (
            <table className="min-w-full bg-white">
              <thead>
                <tr className="bg-gray-100 text-gray-700 text-sm">
                  <th className="py-3 px-4 text-left">
                    <Checkbox
                      checked={selectAll}
                      onCheckedChange={(checked) => setSelectAll(!!checked)}
                    />
                  </th>
                  <th className="py-3 px-4 text-left">File Name</th>
                  <th className="py-3 px-4 text-left">Size</th>
                  <th className="py-3 px-4 text-left">Upload Date</th>
                  <th className="py-3 px-4 text-left">Type</th>
                  <th className="py-3 px-4 text-left">Actions</th>
                </tr>
              </thead>
              <tbody>
                {paginatedFiles.map((file: any) => (
                  <tr key={file.name} className="border-b border-gray-200 hover:bg-gray-50">
                    <td className="py-3 px-4">
                      <Checkbox
                        checked={selectedFiles.includes(file.name)}
                        onCheckedChange={(checked) => handleFileSelect(file.name, !!checked)}
                      />
                    </td>
                    <td className="py-3 px-4 font-medium">{file.name}</td>
                    <td className="py-3 px-4">{formatBytes(file.size)}</td>
                    <td className="py-3 px-4">{formatDate(file.lastModified)}</td>
                    <td className="py-3 px-4">
                      <Badge variant="outline" className="bg-blue-100 text-blue-800 border-blue-200">
                        {getFileTypeLabel(file.name)}
                      </Badge>
                    </td>
                    <td className="py-3 px-4">
                      <div className="flex space-x-2">
                        <Button variant="ghost" size="icon" className="text-primary h-8 w-8 p-1">
                          <PlayCircle className="h-5 w-5" />
                        </Button>
                        <Button variant="ghost" size="icon" className="text-gray-600 h-8 w-8 p-1">
                          <Download className="h-5 w-5" />
                        </Button>
                        <Button variant="ghost" size="icon" className="text-gray-600 h-8 w-8 p-1">
                          <MoreVertical className="h-5 w-5" />
                        </Button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        
        {filteredFiles.length > 0 && (
          <div className="flex justify-between items-center mt-4">
            <div className="text-sm text-gray-500">
              Showing {Math.min(paginatedFiles.length, itemsPerPage)} of {filteredFiles.length} files
            </div>
            <div className="flex space-x-1">
              <Button 
                variant="outline" 
                size="sm" 
                disabled={currentPage === 1}
                onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
              >
                &laquo; Previous
              </Button>
              
              {[...Array(Math.min(totalPages, 3))].map((_, i) => {
                // Show current page and adjacent pages
                let pageToShow = currentPage;
                if (i === 0) pageToShow = Math.max(1, currentPage - 1);
                if (i === 2) pageToShow = Math.min(totalPages, currentPage + 1);
                
                return (
                  <Button
                    key={i}
                    variant={pageToShow === currentPage ? "default" : "outline"}
                    size="sm"
                    onClick={() => setCurrentPage(pageToShow)}
                  >
                    {pageToShow}
                  </Button>
                );
              })}
              
              <Button 
                variant="outline" 
                size="sm" 
                disabled={currentPage === totalPages || totalPages === 0}
                onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
              >
                Next &raquo;
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
