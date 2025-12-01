import React from 'react';
import { NotebookOutput } from '../types';
import { CheckCircle2, ArrowLeft, Terminal, CloudLightning } from 'lucide-react';

interface ResultsDashboardProps {
  data: NotebookOutput | null;
  onReset: () => void;
}

export const ResultsDashboard: React.FC<ResultsDashboardProps> = ({ data, onReset }) => {
  if (!data) return null;

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

      {/* Action Button */}
      <button
        onClick={onReset}
        className="group px-6 py-3 bg-white text-slate-700 border border-slate-300 rounded-lg hover:bg-slate-50 hover:border-slate-400 transition-all font-medium flex items-center gap-2 focus:ring-4 focus:ring-slate-100"
      >
        <ArrowLeft size={18} className="group-hover:-translate-x-1 transition-transform" />
        Configure New Run
      </button>
    </div>
  );
};