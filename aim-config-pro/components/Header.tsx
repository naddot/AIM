import React from 'react';
import { Target } from 'lucide-react';

export const Header: React.FC = () => {
  return (
    <header className="bg-white-700 text-black shadow-lg sticky top-0 z-50">
      <div className="bg-slate-100 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-[#00ab04] rounded-lg">
            <Target size={25} className="text-white" />
          </div>
          <div>
            <h1 className="text-black text-xl font-bold tracking-tight">AIM Workspace</h1>
          </div>
        </div>
        <div className="hidden md:flex items-center gap-4 text-sm text-slate-400">
          <span className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-green-500"></div>
            System Active
          </span>
        </div>
      </div>
    </header>
  );
};
