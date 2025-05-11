import { useState, useEffect } from "react";
import { useTheme } from "next-themes";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Button } from "@/components/ui/button";
import { PaletteIcon, SunIcon, MoonIcon, ZapIcon, LaptopIcon, SparklesIcon } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

type ThemeType = "light" | "dark" | "calm" | "energetic" | "professional" | "creative";

interface ThemeOption {
  value: ThemeType;
  label: string;
  icon: React.ReactNode;
  tooltip: string;
}

export function ThemeSelector() {
  const { theme, setTheme } = useTheme();
  const [mounted, setMounted] = useState(false);

  // After mounting, we can safely show the UI
  useEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted) {
    return null;
  }

  const themeOptions: ThemeOption[] = [
    { 
      value: "light", 
      label: "Light", 
      icon: <SunIcon className="h-4 w-4" />, 
      tooltip: "Standard light theme"
    },
    { 
      value: "dark", 
      label: "Dark", 
      icon: <MoonIcon className="h-4 w-4" />,
      tooltip: "Standard dark theme"
    },
    { 
      value: "calm", 
      label: "Calm", 
      icon: <SparklesIcon className="h-4 w-4" />,
      tooltip: "Soothing blues for a relaxed mood"
    },
    { 
      value: "energetic", 
      label: "Energetic", 
      icon: <ZapIcon className="h-4 w-4" />,
      tooltip: "Vibrant orange and red theme for focus"
    },
    { 
      value: "professional", 
      label: "Professional", 
      icon: <LaptopIcon className="h-4 w-4" />,
      tooltip: "Clean professional appearance"
    },
    { 
      value: "creative", 
      label: "Creative", 
      icon: <SparklesIcon className="h-4 w-4" />,
      tooltip: "Inspiring purple and pink tones"
    },
  ];

  // Find current theme option
  const currentTheme = themeOptions.find(option => option.value === theme) || themeOptions[0];

  return (
    <DropdownMenu>
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon" aria-label="Select a theme">
                <PaletteIcon className="h-5 w-5" />
              </Button>
            </DropdownMenuTrigger>
          </TooltipTrigger>
          <TooltipContent>
            <p>Change color theme</p>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
      
      <DropdownMenuContent align="end">
        {themeOptions.map((option) => (
          <DropdownMenuItem 
            key={option.value}
            onClick={() => {
              // Apply both theme class and next-themes system
              if (option.value === "light" || option.value === "dark") {
                setTheme(option.value);
              } else {
                // For mood-based themes, we apply a CSS class and use light as the base
                document.documentElement.classList.remove(
                  "theme-calm", 
                  "theme-energetic", 
                  "theme-professional", 
                  "theme-creative"
                );
                document.documentElement.classList.add(`theme-${option.value}`);
                setTheme("light"); // Keep the light theme as base
              }
            }}
            className="flex items-center gap-2"
          >
            <div className={`${theme === option.value ? "text-primary" : ""}`}>
              {option.icon}
            </div>
            <span className={`${theme === option.value ? "font-medium" : ""}`}>{option.label}</span>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}