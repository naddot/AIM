import React, { useEffect, useState } from 'react';

interface JobProgressViewProps {
    status: 'checking' | 'active' | 'error';
    jobDetails: {
        executionId: string | null;
        startTime: string | null;
        state: string | null; // e.g. "RUNNING"
        currentStage?: string;
        logs?: Array<{ time: string, level: string, msg: string }>;
    };
    onAbort?: () => void;
}

const steps = [
    { id: 'stage1', label: 'Stage 1: S3->BQ', status: 'pending' },
    { id: 'stage3', label: 'Stage 3: TyreScore', status: 'pending' },
    { id: 'stage4', label: 'Stage 4: Batch/Override', status: 'pending' },
    { id: 'stage5', label: 'Stage 5: Dashboard', status: 'pending' },
    { id: 'stage6', label: 'Stage 6: Insights', status: 'pending' },
    { id: 'stage7', label: 'Stage 7: Analysis', status: 'pending' },
    { id: 'stage8', label: 'Stage 8: Merchandising', status: 'pending' },
    { id: 'stage9', label: 'Stage 9: Size File', status: 'pending' },
];

export const JobProgressView: React.FC<JobProgressViewProps> = ({ status, jobDetails, onAbort }) => {
    const [elapsed, setElapsed] = useState<string>('0m 0s');

    // Use real logs if available, else fall back to initial state (empty)
    // We do NOT use mock logs anymore if real data is requested.
    // Although for "mock=true" url param, the App.tsx might pass mock data.
    // Here we just render what we get.
    const logs = jobDetails.logs || [];

    // Determine Step Status
    const currentStageId = jobDetails.currentStage || 'stage1';

    // Logic: 
    // - steps before current -> completed
    // - current step -> active
    // - steps after current -> pending
    const stepsWithStatus = steps.map(step => {
        const stepIndex = steps.findIndex(s => s.id === step.id);
        const currentIndex = steps.findIndex(s => s.id === currentStageId);

        let stepStatus = 'pending';
        if (stepIndex < currentIndex) stepStatus = 'completed';
        else if (stepIndex === currentIndex) stepStatus = 'active';

        return { ...step, status: stepStatus };
    });



    useEffect(() => {
        if (!jobDetails.startTime) return;
        const interval = setInterval(() => {
            const start = new Date(jobDetails.startTime!).getTime();
            const now = new Date().getTime();
            const diff = Math.max(0, Math.floor((now - start) / 1000));
            const m = Math.floor(diff / 60);
            const s = diff % 60;
            setElapsed(`${m}m ${s}s`);
        }, 1000);
        return () => clearInterval(interval);
    }, [jobDetails.startTime]);

    return (
        <div className="flex flex-col gap-6 animate-in fade-in zoom-in-95 duration-500">

            {/* Header Section (Light Card) */}
            <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-6 flex flex-col md:flex-row justify-between items-center gap-4">
                <div className="flex items-center gap-4">
                    <div className="relative">
                        <span className="material-symbols-outlined text-4xl text-[#00ab04] animate-spin">settings</span>
                        <div className="absolute inset-0 flex items-center justify-center">
                            <div className="w-4 h-4 bg-white rounded-full"></div>
                        </div>
                    </div>
                    <div>
                        <h2 className="text-xl font-bold text-slate-900">
                            {status === 'checking' ? 'Checking Status...' : 'Processing Data'}
                        </h2>
                        <div className="flex items-center gap-2 text-sm text-slate-500 font-medium">
                            <span className="inline-block w-2 h-2 rounded-full bg-[#00ab04] animate-pulse"></span>
                            <span>Job #{jobDetails.executionId?.slice(-6) || 'UNKNOWN'}</span>
                            <span>•</span>
                            <span>Started {elapsed} ago</span>
                        </div>
                    </div>
                </div>

                <button
                    onClick={onAbort}
                    className="px-4 py-2 bg-red-50 text-red-600 font-semibold rounded-lg border border-red-100 hover:bg-red-100 transition-colors flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
                    disabled={true} // Not implemented yet
                >
                    <span className="material-symbols-outlined text-lg">cancel</span>
                    Abort Job
                </button>
            </div>

            {/* Progress Steps (Horizontal Scroll) */}
            <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-6 overflow-x-auto">
                <div className="flex items-center min-w-max gap-4 px-2">
                    {stepsWithStatus.map((step, idx) => (
                        <React.Fragment key={step.id}>
                            {/* Step Item */}
                            <div className={`flex flex-col items-center gap-2 min-w-[80px] ${step.status === 'pending' ? 'opacity-40 grayscale' : 'opacity-100'}`}>
                                <div className={`
                                w-10 h-10 rounded-full flex items-center justify-center border-2 transition-all
                                ${step.status === 'completed' ? 'bg-[#00ab04] border-[#00ab04] text-white shadow-sm' : ''}
                                ${step.status === 'active' ? 'bg-white border-[#00ab04] text-[#00ab04] ring-4 ring-green-50' : ''}
                                ${step.status === 'pending' ? 'bg-slate-50 border-slate-200 text-slate-400' : ''}
                             `}>
                                    {step.status === 'completed' && <span className="material-symbols-outlined text-lg">check</span>}
                                    {step.status === 'active' && <span className="material-symbols-outlined text-lg animate-spin">sync</span>}
                                    {step.status === 'pending' && <span className="material-symbols-outlined text-lg">circle</span>}
                                </div>
                                <span className="text-xs font-semibold text-slate-700 whitespace-nowrap">{step.label.split(':')[0]}</span>
                            </div>

                            {/* Connector Line (except last) */}
                            {idx < steps.length - 1 && (
                                <div className={`h-1 w-12 rounded-full flex-shrink-0 ${step.status === 'completed' ? 'bg-[#00ab04]' : 'bg-slate-100'}`}></div>
                            )}
                        </React.Fragment>
                    ))}
                </div>
            </div>



            {/* Logs */}
            <div className="bg-slate-900 rounded-xl shadow-md border border-slate-800 overflow-hidden flex flex-col h-64">
                <div className="bg-slate-950 px-4 py-2 border-b border-slate-800 flex justify-between items-center">
                    <span className="text-xs font-mono font-semibold text-slate-400 flex items-center gap-2">
                        <span className="material-symbols-outlined text-sm">terminal</span>
                        LIVE LOGS
                    </span>
                </div>
                <div className="flex-1 p-4 font-mono text-xs overflow-y-auto space-y-1">
                    {logs.map((log, i) => (
                        <div key={i} className="flex gap-3 text-slate-300">
                            <span className="text-slate-500 select-none">[{log.time}]</span>
                            <span className="text-blue-400 font-bold">{log.level}</span>
                            <span>{log.msg}</span>
                        </div>
                    ))}
                    <div className="animate-pulse text-[#00ab04]">_</div>
                </div>
            </div>

        </div>
    );
};
