import React from 'react';
import { Shield, Users, Zap } from 'lucide-react';

export const Principles: React.FC = () => {
  return (
    <div className="mt-12 mb-12 mx-auto text-center max-w-2xl px-4">
      <h3 className="text-slate-900 font-bold text-lg mb-6">Our Principles</h3>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-8 text-sm">
        <div className="flex flex-col items-center gap-2">
          <div className="w-12 h-12 rounded-full bg-slate-900 text-white flex items-center justify-center shadow-md">
            <Shield className="w-6 h-6" />
          </div>
          <span className="font-bold text-base text-slate-900">Privacy</span>
          <span className="text-slate-500">Privacy first</span>
        </div>
        <div className="flex flex-col items-center gap-2">
          <div className="w-12 h-12 rounded-full bg-[#00ab04] text-white flex items-center justify-center shadow-md">
            <Users className="w-6 h-6" />
          </div>
          <span className="font-bold text-base text-slate-900">People</span>
          <span className="text-slate-500">People centric</span>
        </div>
        <div className="flex flex-col items-center gap-2">
          <div className="w-12 h-12 rounded-full bg-white border-2 border-slate-900 text-slate-900 flex items-center justify-center shadow-md">
            <Zap className="w-6 h-6" />
          </div>
          <span className="font-bold text-base text-slate-900">Performance</span>
          <span className="text-slate-500">Results focused</span>
        </div>
      </div>
    </div>
  );
};
