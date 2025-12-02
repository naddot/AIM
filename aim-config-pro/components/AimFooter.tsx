import React from 'react';
import { Target } from 'lucide-react';

export const AimFooter: React.FC = () => {
  return (
    <footer className="bg-slate-900 text-white mt-auto py-12">
      <div className="max-w-7xl mx-auto px-4 text-center">
        <div className="flex justify-center items-center gap-2 mb-4">
          <div className="w-8 h-8 bg-[#00ab04] rounded-lg flex items-center justify-center text-slate-900">
            <Target size={25} className="text-white" />
          </div>
          <span className="font-bold text-xl tracking-tight">AIM Workspace</span>
        </div>
        <p className="text-slate-300 text-sm font-medium">
          &copy; {new Date().getFullYear()} AIM Workspace powered by Nexus
        </p>
      </div>
    </footer>
  );
};
