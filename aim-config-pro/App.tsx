import React, { useState } from 'react';
import { InputForm } from './components/InputForm';
import { ResultsDashboard } from './components/ResultsDashboard';
import { HeroSection } from './components/HeroSection';
import { Principles } from './components/Principles';
import { AimFooter } from './components/AimFooter';
import { FinsightFooter } from './components/FinsightFooter';
import { Header } from './components/Header';
import { NotebookParams, NotebookOutput } from './types';
import { saveConfigurationAndRun } from './services/gemini';
import { AlertTriangle } from 'lucide-react';

const App: React.FC = () => {
  const [data, setData] = useState<NotebookOutput | null>(null);
  const [lastConfig, setLastConfig] = useState<NotebookParams | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [hasRun, setHasRun] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSaveAndRun = async (params: NotebookParams) => {
    setIsLoading(true);
    setError(null);
    setHasRun(false); // Reset previous run state while loading
    setLastConfig(params);
    
    console.group("🏁 App: Starting Save Process");
    try {
      const result = await saveConfigurationAndRun(params);
      setData(result);
      setHasRun(true);
    } catch (err: any) {
      console.error("App Caught Error:", err);
      // Display the actual error message thrown by the service (which might come from the backend)
      setError(err.message || "Failed to connect to the cloud endpoint. Please ensure the Cloud Function URL is correct and accessible.");
    } finally {
      setIsLoading(false);
      console.groupEnd();
    }
  };

  const handleReset = () => {
    setHasRun(false);
    setData(null);
    setError(null);
    setLastConfig(null);
  };

  return (
    <div className="min-h-screen flex flex-col bg-slate-100 text-slate-900">
      
      <Header />

      {/* Main Content */}
      <main className="flex-1 w-full max-w-3xl mx-auto px-4 py-8 flex flex-col gap-6 justify-center">
        <HeroSection />
        
        {error && (
          <div className="bg-red-50 border-l-4 border-red-500 p-4 rounded-r-lg shadow-sm animate-in fade-in slide-in-from-top-2">
            <div className="flex items-start gap-3">
                <div className="flex-shrink-0 mt-0.5">
                    <AlertTriangle className="h-5 w-5 text-red-500" />
                  </div>
                  <div className="w-full">
                    <h3 className="text-sm font-medium text-red-800 mb-1">Process Failed</h3>
                    <div className="text-sm text-red-700 bg-red-100/50 p-2 rounded border border-red-200 font-mono whitespace-pre-wrap break-words">
                      {error}
                    </div>
                    <p className="mt-2 text-xs text-red-500 opacity-75">
                      Check the browser console (F12) for "Raw Response Body" to see full details.
                    </p>
                  </div>
            </div>
          </div>
        )}

        {!hasRun ? (
          <InputForm onSubmit={handleSaveAndRun} isLoading={isLoading} />
        ) : (
          <ResultsDashboard data={data} config={lastConfig} onReset={handleReset} />
        )}

      </main>

      <Principles />
      <AimFooter />
      {/* Swap to the alternate footer by uncommenting below */}
      {/* <FinsightFooter | AimFooter /> */}
    </div>
  );
};

export default App;
