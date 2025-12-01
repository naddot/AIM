import React from 'react';
import { Shield, Users, Zap, Images, Target } from 'lucide-react';

export const PrinciplesFooter: React.FC = () => {
  return (
    <>
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
    </>
  );
};
