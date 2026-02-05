/// <reference types="vite/client" />
import React from 'react';
import { NotebookOutput, NotebookParams } from '../types';
import { CheckCircle2, ArrowLeft, Terminal, CloudLightning } from 'lucide-react';

interface ResultsDashboardProps {
  data: NotebookOutput | null;
  config: NotebookParams | null;
  onReset: () => void;
}

const CONFIG_LABELS: Record<keyof NotebookParams, string> = {
  RUN_MODE: "Run Mode",
  TOTAL_PER_SEGMENT: "Total Per Segment",
  TOTAL_OVERALL: "Total Overall",
  BATCH_SIZE: "Batch Size",
  GOLDILOCKS_ZONE_PCT: "Goldilocks Zone (%)",
  PRICE_FLUCTUATION_UPPER: "Price Fluctuation Upper",
  PRICE_FLUCTUATION_LOWER: "Price Fluctuation Lower",
  BRAND_ENHANCER: "Brand Enhancer",
  MODEL_ENHANCER: "Model Enhancer",
  SEASON: "Season",
  LIMIT_TO_SEGMENTS: "Limit To Segments",
};

export const ResultsDashboard: React.FC<ResultsDashboardProps> = ({ data, config, onReset }) => {
  if (!data) return null;

  const configEntries = config
    ? (Object.keys(CONFIG_LABELS) as (keyof NotebookParams)[]).flatMap((key) => {
      let val: string | number | undefined = (config as any)[key];

      // Logic to hide irrelevant keys based on mode
      if (config.RUN_MODE === "GLOBAL") {
        if (key === "TOTAL_PER_SEGMENT") return []; // Hide
      } else {
        // PER_SEGMENT
        if (key === "TOTAL_OVERALL") return []; // Hide
        // We might also hide BATCH_SIZE if it's not relevant for Per-Segment, but it is locked to 50 in InputForm anyway.
      }

      if (key === "LIMIT_TO_SEGMENTS") {
        val = val ? val : "All segments";
      } else if (key === "SEASON") {
        val = val || "None";
      }

      // If value is undefined (because we cleaned payload and types are optional/loose), skip or show placeholder.
      if (val === undefined || val === null) {
        return [];
      }

      return [{ label: CONFIG_LABELS[key], value: val }];
    })
    : [];

  const [isJobRunning, setIsJobRunning] = React.useState(false);
  const [jobResult, setJobResult] = React.useState<{ success: boolean; message: string } | null>(null);

  const handleRunJob = async () => {
    setIsJobRunning(true);
    setJobResult(null);
    try {
      const res = await fetch('/api/trigger-job', { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        setJobResult({ success: true, message: "Job triggered successfully! It may take a few minutes to complete." });
      } else {
        setJobResult({ success: false, message: data.error || "Failed to trigger job." });
      }
    } catch (err) {
      setJobResult({ success: false, message: "Network error. Failed to trigger job." });
    } finally {
      setIsJobRunning(false);
    }
  };

  return (
    <div className="bg-white rounded-xl shadow-xl border border-slate-200 p-8 md:p-12 flex flex-col items-center text-center animate-in fade-in zoom-in duration-500 fill-mode-both">

      {/* Success Icon */}
      <div className="w-24 h-24 bg-green-50 rounded-full flex items-center justify-center mb-8 shadow-inner ring-8 ring-green-50/50">
        <CheckCircle2 size={48} className="text-green-600" />
      </div>

      {/* Title */}
      <h2 className="text-3xl font-bold text-slate-900 mb-3 tracking-tight">Configuration Updated</h2>
      <p className="text-slate-600 mb-8 max-w-md text-lg leading-relaxed">
        Your parameters have been securely transmitted to the cloud.
      </p>

      {/* API Response Card */}
      <div className="w-full max-w-lg bg-slate-900 rounded-lg overflow-hidden shadow-lg mb-8 text-left border border-slate-800">
        <div className="bg-slate-800 px-4 py-2 flex items-center gap-2 border-b border-slate-700">
          <div className="flex gap-1.5">
            <div className="w-2.5 h-2.5 rounded-full bg-red-500"></div>
            <div className="w-2.5 h-2.5 rounded-full bg-yellow-500"></div>
            <div className="w-2.5 h-2.5 rounded-full bg-green-500"></div>
          </div>
          <div className="ml-2 text-xs font-mono text-slate-400 flex items-center gap-1">
            <CloudLightning size={10} />
            cloud_run_response.log
          </div>
        </div>
        <div className="p-6 font-mono text-sm">
          <div className="flex gap-2 text-green-400 mb-2">
            <span className="opacity-50">$</span>
            <span>STATUS: 200 OK</span>
          </div>
          <div className="text-slate-300 mb-4 whitespace-pre-wrap">
            {data.message}
          </div>
          <div className="text-slate-500 text-xs pt-4 border-t border-slate-800/50 flex justify-between">
            <span>TIMESTAMP</span>
            <span>{data.timestamp}</span>
          </div>
        </div>
      </div>

      {/* Submitted Config */}
      {configEntries.length > 0 && (
        <div className="w-full max-w-lg bg-white border border-slate-200 rounded-lg p-4 mb-6 text-left shadow-sm">
          <div className="text-xs font-semibold text-slate-500 uppercase mb-2">Submitted Configuration</div>
          <dl className="grid grid-cols-1 sm:grid-cols-2 gap-y-2 gap-x-4 text-sm">
            {configEntries.map(({ label, value }) => (
              <div key={label} className="flex flex-col">
                <dt className="text-xs uppercase text-slate-500 font-semibold">{label}</dt>
                <dd className="text-slate-900">{String(value)}</dd>
              </div>
            ))}
          </dl>
        </div>
      )}

      {/* Job Trigger Result */}
      {jobResult && (
        <div
          className={`w-full max-w-lg p-4 mb-6 rounded-lg text-left border ${jobResult.success ? 'bg-green-50 border-green-200 text-green-800' : 'bg-red-50 border-red-200 text-red-800'
            }`}
        >
          <div className="flex items-center gap-2 font-semibold">
            {jobResult.success ? <CheckCircle2 size={18} /> : <Terminal size={18} />}
            {jobResult.success ? "Success" : "Error"}
          </div>
          <p className="text-sm mt-1">{jobResult.message}</p>
        </div>
      )}

      {/* Action Buttons */}
      <div className="flex flex-col sm:flex-row gap-3 w-full max-w-lg">
        <button
          onClick={handleRunJob}
          disabled={isJobRunning}
          className="flex-1 px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-all font-medium flex items-center justify-center gap-2 shadow-md disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {isJobRunning ? (
            <>
              <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              Starting Job...
            </>
          ) : (
            <>
              <Terminal size={18} />
              Run AIM Job Now
            </>
          )}
        </button>

        <button
          onClick={onReset}
          className="flex-1 px-6 py-3 bg-white text-slate-700 border border-slate-300 rounded-lg hover:bg-slate-50 hover:border-slate-400 transition-all font-medium flex items-center justify-center gap-2 focus:ring-4 focus:ring-slate-100"
        >
          <ArrowLeft size={18} />
          Configure New Run
        </button>
      </div>
    </div>
  );
};