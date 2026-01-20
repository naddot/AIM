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
      </div>
    </header>
  );
};
