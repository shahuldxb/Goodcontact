import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import NotFound from "@/pages/not-found";
import Dashboard from "@/pages/dashboard";
import Analysis from "@/pages/analysis";
import Settings from "@/pages/settings";
import Debug from "@/pages/debug";
import { Sidebar } from "@/components/layout/sidebar";
import { Header } from "@/components/layout/header";

function Router() {
  return (
    <div className="min-h-screen flex flex-col">
      <Header />
      <div className="flex flex-1 pt-16">
        <Sidebar />
        <Switch>
          <Route path="/" component={Dashboard} />
          <Route path="/analysis" component={Analysis} />
          <Route path="/analysis/:fileid" component={Analysis} />
          <Route path="/settings" component={Settings} />
          <Route path="/debug" component={Debug} />
          <Route component={NotFound} />
        </Switch>
      </div>
    </div>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Router />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
