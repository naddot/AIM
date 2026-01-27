import React, { useState, useEffect } from 'react';
import { InputForm } from './components/InputForm';
import { ResultsDashboard } from './components/ResultsDashboard';
import { HeroSection } from './components/HeroSection';
import { Principles } from './components/Principles';
import { AimFooter } from './components/AimFooter';
import { FinsightFooter } from './components/FinsightFooter';
import { Header } from './components/Header';
import { JobProgressView } from './components/JobProgressView';
import { NotebookParams, NotebookOutput } from './types';
import { saveConfigurationAndRun, getJobStatus } from './services/gemini';
import { AlertTriangle } from 'lucide-react';

const App: React.FC = () => {
  const [data, setData] = useState<NotebookOutput | null>(null);
  const [lastConfig, setLastConfig] = useState<NotebookParams | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [hasRun, setHasRun] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Job Status State
  const [appStatus, setAppStatus] = useState<'checking' | 'idle' | 'active'>('checking');
  const [jobDetails, setJobDetails] = useState<{
    executionId: string | null;
    startTime: string | null;
    state: string | null;
  }>({ executionId: null, startTime: null, state: null });

  // Poll for status
  useEffect(() => {
    let intervalId: NodeJS.Timeout;

    const checkStatus = async () => {
      // MOCK MODE: Check for query param ?mock=true
      const urlParams = new URLSearchParams(window.location.search);
      if (urlParams.get('mock') === 'true') {
        setAppStatus('active');
        setJobDetails({
          executionId: "job-mock-12345",
          startTime: new Date(Date.now() - 120000).toISOString(), // Started 2 mins ago
          state: "RUNNING"
        });
        return;
      }

      try {
        const status = await getJobStatus();
        if (status.isActive) {
          setAppStatus('active');
          setJobDetails({
            executionId: status.executionId,
            startTime: status.startTime,
            state: status.state
          });
        } else {
          // If we were active and now we are not, we could switch to idle.
          // However, if we just finished a run initiated by THIS user, we might want to stay on Results.
          // For now, if active -> idle transition happens, we go to idle (InputForm) unless we have local results.
          if (appStatus !== 'idle') {
            setAppStatus('idle');
          }
        }
      } catch (e) {
        console.error("Polling failed", e);
        if (appStatus === 'checking') setAppStatus('idle'); // Fallback
      }
    };

    // Initial check
    checkStatus();

    // Set up polling if active
    if (appStatus === 'active') {
      intervalId = setInterval(checkStatus, 5000);
    } else {
      // Poll less frequently if idle, just to catch external triggers? 
      // Or maybe don't poll if idle to save resources, only check on mount/focus.
      // Let's poll every 30s if idle just in case.
      intervalId = setInterval(checkStatus, 30000);
    }

    return () => clearInterval(intervalId);
  }, [appStatus]);

  const handleSaveAndRun = async (params: NotebookParams) => {
    setIsLoading(true);
    setError(null);
    setHasRun(false);
    setLastConfig(params);

    console.group("🏁 App: Starting Save Process");
    try {
      const result = await saveConfigurationAndRun(params);
      setData(result);
      setHasRun(true);
    } catch (err: any) {
      console.error("App Caught Error:", err);

      // Check if it's a conflict (409) -> Job already running
      // The service throws generic Error currently, we might need to parse it or update service to throw structured error.
      // For now, if the message contains "already running", switch view.
      if (err.message && err.message.includes("already running")) {
        // Force a status check immediately
        setAppStatus('active');
        setError("Job was started in another session. Switching to progress view...");
      } else {
        setError(err.message || "Failed to trigger job.");
      }
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

  if (appStatus === 'checking') {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-100 text-slate-500 font-sans">
        <div className="flex flex-col items-center gap-4">
          <span className="material-symbols-outlined animate-spin text-4xl text-primary">sync</span>
          <p className="font-medium animate-pulse">Checking system status...</p>
        </div>
      </div>
    );
  }

  if (appStatus === 'active') {
    return (
      <div className="min-h-screen bg-slate-100">
        <JobProgressView
          status="active"
          jobDetails={jobDetails}
          onAbort={() => { /* TODO */ }}
        />
      </div>
    );
  }

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
    </div>
  );
};

export default App;
