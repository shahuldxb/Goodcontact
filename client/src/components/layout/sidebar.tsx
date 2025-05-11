import { useState } from "react";
import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";

export function Sidebar() {
  const [location] = useLocation();
  const [isMobileSidebarOpen, setIsMobileSidebarOpen] = useState(false);

  const navItems = [
    { label: "Dashboard", path: "/", icon: "bi-house-door" },
    { label: "Transcriptions", path: "/analysis", icon: "bi-file-earmark-text" },
    { label: "Analytics", path: "/analytics", icon: "bi-graph-up" },
  ];

  const analysisItems = [
    { label: "Sentiment Analysis", path: "/sentiment", icon: "bi-emoji-smile" },
    { label: "Language Detection", path: "/language", icon: "bi-translate" },
    { label: "Call Summarization", path: "/summary", icon: "bi-file-text" },
    { label: "Forbidden Phrases", path: "/forbidden", icon: "bi-shield-exclamation" },
    { label: "Topic Modeling", path: "/topics", icon: "bi-chat-square-text" },
    { label: "Speaker Diarization", path: "/speakers", icon: "bi-people" },
  ];

  const toggleMobileSidebar = () => setIsMobileSidebarOpen(!isMobileSidebarOpen);

  return (
    <>
      {/* Desktop Sidebar */}
      <aside className="w-64 bg-white shadow-md fixed h-full hidden md:block z-10">
        <div className="py-4">
          <ul className="mt-2">
            <li className="px-4 py-2 text-sm font-medium text-gray-600">MAIN</li>
            {navItems.map((item) => (
              <li key={item.path}>
                <Link href={item.path}>
                  <a className={cn(
                    "flex items-center px-4 py-3 text-gray-800 hover:bg-gray-100",
                    { "bg-primary bg-opacity-10 text-primary border-l-3 border-primary": location === item.path }
                  )}>
                    <i className={cn("bi", item.icon, "mr-3 text-gray-600", { "text-primary": location === item.path })}></i>
                    {item.label}
                  </a>
                </Link>
              </li>
            ))}
            
            <li className="px-4 py-2 text-sm font-medium text-gray-600 mt-4">ANALYSIS</li>
            {analysisItems.map((item) => (
              <li key={item.path}>
                <Link href={item.path}>
                  <a className={cn(
                    "flex items-center px-4 py-3 text-gray-800 hover:bg-gray-100",
                    { "bg-primary bg-opacity-10 text-primary border-l-3 border-primary": location === item.path }
                  )}>
                    <i className={cn("bi", item.icon, "mr-3 text-gray-600", { "text-primary": location === item.path })}></i>
                    {item.label}
                  </a>
                </Link>
              </li>
            ))}
            
            <li className="px-4 py-2 text-sm font-medium text-gray-600 mt-4">SETTINGS</li>
            <li>
              <Link href="/settings">
                <a className={cn(
                  "flex items-center px-4 py-3 text-gray-800 hover:bg-gray-100",
                  { "bg-primary bg-opacity-10 text-primary border-l-3 border-primary": location === "/settings" }
                )}>
                  <i className={cn("bi bi-gear mr-3 text-gray-600", { "text-primary": location === "/settings" })}></i>
                  Configuration
                </a>
              </Link>
            </li>
            
            <li className="px-4 py-2 text-sm font-medium text-gray-600 mt-4">DEVELOPER</li>
            <li>
              <Link href="/debug">
                <a className={cn(
                  "flex items-center px-4 py-3 text-gray-800 hover:bg-gray-100",
                  { "bg-primary bg-opacity-10 text-primary border-l-3 border-primary": location === "/debug" }
                )}>
                  <i className={cn("bi bi-bug mr-3 text-gray-600", { "text-primary": location === "/debug" })}></i>
                  Debug Tools
                </a>
              </Link>
            </li>
          </ul>
        </div>
      </aside>
      
      {/* Mobile Navigation Toggle */}
      <div className="md:hidden fixed bottom-4 right-4 z-10">
        <button
          onClick={toggleMobileSidebar}
          className="bg-primary text-white rounded-full w-12 h-12 flex items-center justify-center shadow-lg"
        >
          <i className="bi bi-list text-xl"></i>
        </button>
      </div>
      
      {/* Mobile Sidebar (hidden by default) */}
      {isMobileSidebarOpen && (
        <div className="fixed inset-0 bg-black bg-opacity-50 z-40 md:hidden">
          <aside className="w-64 bg-white shadow-md h-full z-50 transform transition-all duration-300">
            <div className="flex justify-between items-center p-4 border-b">
              <h2 className="font-semibold">Menu</h2>
              <button onClick={toggleMobileSidebar} className="text-gray-500">
                <i className="bi bi-x-lg"></i>
              </button>
            </div>
            <div className="py-4">
              <ul className="mt-2">
                <li className="px-4 py-2 text-sm font-medium text-gray-600">MAIN</li>
                {navItems.map((item) => (
                  <li key={item.path}>
                    <Link href={item.path}>
                      <a className={cn(
                        "flex items-center px-4 py-3 text-gray-800 hover:bg-gray-100",
                        { "bg-primary bg-opacity-10 text-primary border-l-3 border-primary": location === item.path }
                      )} onClick={toggleMobileSidebar}>
                        <i className={cn("bi", item.icon, "mr-3 text-gray-600", { "text-primary": location === item.path })}></i>
                        {item.label}
                      </a>
                    </Link>
                  </li>
                ))}
                
                <li className="px-4 py-2 text-sm font-medium text-gray-600 mt-4">ANALYSIS</li>
                {analysisItems.map((item) => (
                  <li key={item.path}>
                    <Link href={item.path}>
                      <a className={cn(
                        "flex items-center px-4 py-3 text-gray-800 hover:bg-gray-100",
                        { "bg-primary bg-opacity-10 text-primary border-l-3 border-primary": location === item.path }
                      )} onClick={toggleMobileSidebar}>
                        <i className={cn("bi", item.icon, "mr-3 text-gray-600", { "text-primary": location === item.path })}></i>
                        {item.label}
                      </a>
                    </Link>
                  </li>
                ))}
                
                <li className="px-4 py-2 text-sm font-medium text-gray-600 mt-4">SETTINGS</li>
                <li>
                  <Link href="/settings">
                    <a className={cn(
                      "flex items-center px-4 py-3 text-gray-800 hover:bg-gray-100",
                      { "bg-primary bg-opacity-10 text-primary border-l-3 border-primary": location === "/settings" }
                    )} onClick={toggleMobileSidebar}>
                      <i className={cn("bi bi-gear mr-3 text-gray-600", { "text-primary": location === "/settings" })}></i>
                      Configuration
                    </a>
                  </Link>
                </li>
                
                <li className="px-4 py-2 text-sm font-medium text-gray-600 mt-4">DEVELOPER</li>
                <li>
                  <Link href="/debug">
                    <a className={cn(
                      "flex items-center px-4 py-3 text-gray-800 hover:bg-gray-100",
                      { "bg-primary bg-opacity-10 text-primary border-l-3 border-primary": location === "/debug" }
                    )} onClick={toggleMobileSidebar}>
                      <i className={cn("bi bi-bug mr-3 text-gray-600", { "text-primary": location === "/debug" })}></i>
                      Debug Tools
                    </a>
                  </Link>
                </li>
              </ul>
            </div>
          </aside>
        </div>
      )}
    </>
  );
}
