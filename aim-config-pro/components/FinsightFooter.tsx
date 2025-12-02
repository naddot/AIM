import React from 'react';
import { BarChart3 } from 'lucide-react';

export const FinsightFooter: React.FC = () => {
  return (
    <footer className="bg-bc-black text-white mt-auto py-12">
      <div className="max-w-7xl mx-auto px-4 text-center">
        <div className="flex justify-center items-center gap-2 mb-4">
          <div className="w-8 h-8 bg-bc-green rounded-lg flex items-center justify-center text-bc-black">
            <BarChart3 size={20} />
          </div>
          <span className="font-bold text-xl tracking-tight">Finsight</span>
        </div>
        <p className="text-gray-400 text-sm font-medium">
          &copy; {new Date().getFullYear()} Finsight powered by Nexus
        </p>
      </div>
    </footer>
  );
};
