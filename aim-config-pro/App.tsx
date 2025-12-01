import React, { useState } from 'react';
import { InputForm } from './components/InputForm';
import { ResultsDashboard } from './components/ResultsDashboard';
import { NotebookParams, NotebookOutput } from './types';
import { saveConfigurationAndRun } from './services/gemini';
import { Database, AlertTriangle } from 'lucide-react';

const App: React.FC = () => {
  const [data, setData] = useState<NotebookOutput | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [hasRun, setHasRun] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSaveAndRun = async (params: NotebookParams) => {
    setIsLoading(true);
    setError(null);
    setHasRun(false); // Reset previous run state while loading
    
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
  };

  return (
    <div className="min-h-screen flex flex-col bg-slate-100 text-slate-900">
      
      {/* Header */}
      <header className="bg-slate-900 text-white shadow-lg sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
             <div className="p-2 bg-blue-600 rounded-lg">
                <Database size={20} className="text-white" />
             </div>
             <div>
                <h1 className="text-xl font-bold tracking-tight">Notebook Runner Pro</h1>
                <p className="text-xs text-slate-400">Cloud Data Processing Interface</p>
             </div>
          </div>
          <div className="hidden md:flex items-center gap-4 text-sm text-slate-400">
             <span className="flex items-center gap-2"><div className="w-2 h-2 rounded-full bg-green-500"></div> System Active</span>
             <span>v2.5.4</span>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 w-full max-w-3xl mx-auto px-4 py-8 flex flex-col gap-6 justify-center">
        
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
          <ResultsDashboard data={data} onReset={handleReset} />
        )}

      </main>

    </div>
  );
};

export default App;