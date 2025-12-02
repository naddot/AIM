import React, { useState } from 'react';
import { NotebookParams, Season } from '../types';
import { RotateCcw, Save, XCircle } from 'lucide-react';

interface InputFormProps {
  onSubmit: (data: NotebookParams) => void;
  isLoading: boolean;
}

const initialParams: NotebookParams = {
  TOTAL_PER_SEGMENT: 1000,
  GOLDILOCKS_ZONE_PCT: 15,
  PRICE_FLUCTUATION_UPPER: 1.1,
  PRICE_FLUCTUATION_LOWER: 0.9,
  BRAND_ENHANCER: "",
  MODEL_ENHANCER: "",
  SEASON: Season.None,
  LIMIT_TO_SEGMENTS: "",
};

export const InputForm: React.FC<InputFormProps> = ({ onSubmit, isLoading }) => {
  const [formData, setFormData] = useState<NotebookParams>(initialParams);
  const [errors, setErrors] = useState<{ totalPerSegment?: string }>({});

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value } = e.target;

    if (name === "TOTAL_PER_SEGMENT") {
      const numericValue = Number(value);
      if (numericValue > 5000) {
        setErrors((prev) => ({ ...prev, totalPerSegment: "The limit for this input is 5000" }));
        return; // Do not allow value beyond 5000
      } else {
        setErrors((prev) => ({ ...prev, totalPerSegment: undefined }));
      }
    }

    setFormData(prev => ({
      ...prev,
      [name]: e.target.type === 'number' ? Number(value) : value,
    }));
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSubmit(formData);
  };

  const handleReset = () => {
    setFormData(initialParams);
    setErrors({});
  };

  // Allow forcing a reload if stuck
  const handleReload = () => {
    window.location.reload();
  };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden flex flex-col h-full">
      <div className="p-6 bg-[#00ab04] border-b border-slate-200">
        <h2 className="text-lg font-semibold text-white flex items-center gap-2">
          Configuration
        </h2>
        <p className="text-sm text-white mt-1">
          Adjust parameters to customize AIM execution run.
        </p>
      </div>

      <form onSubmit={handleSubmit} className="p-6 flex-1 overflow-y-auto space-y-6">

        {/* Metric Inputs */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="space-y-2">
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Total Per Segment
            </label>
            <input
              type="number"
              name="TOTAL_PER_SEGMENT"
              value={formData.TOTAL_PER_SEGMENT}
              onChange={handleChange}
              className="w-full px-4 py-2 bg-slate-50 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition-all"
              required
            />
            {errors.totalPerSegment && (
              <p className="text-xs text-red-600">{errors.totalPerSegment}</p>
            )}
          </div>

          <div className="space-y-2">
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Goldilocks Zone (%)
            </label>
            <div className="relative">
              <input
                type="number"
                name="GOLDILOCKS_ZONE_PCT"
                value={formData.GOLDILOCKS_ZONE_PCT}
                onChange={handleChange}
                className="w-full px-4 py-2 bg-slate-50 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition-all pr-8"
                required
              />
              <span className="absolute right-3 top-2 text-slate-400">%</span>
            </div>
          </div>
        </div>

        {/* Fluctuation Range */}
        <div className="space-y-3">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Price Fluctuation Range
          </label>
          <div className="grid grid-cols-2 gap-4 bg-slate-50 p-4 rounded-lg border border-slate-200">
            <div className="space-y-1">
              <span className="text-xs text-slate-400">Lower Bound</span>
              <input
                type="number"
                name="PRICE_FLUCTUATION_LOWER"
                value={formData.PRICE_FLUCTUATION_LOWER}
                onChange={handleChange}
                step="0.1"
                className="w-full px-3 py-1.5 bg-white border border-slate-300 rounded focus:ring-1 focus:ring-blue-500 outline-none"
              />
            </div>
            <div className="space-y-1">
              <span className="text-xs text-slate-400">Upper Bound</span>
              <input
                type="number"
                name="PRICE_FLUCTUATION_UPPER"
                value={formData.PRICE_FLUCTUATION_UPPER}
                onChange={handleChange}
                step="0.1"
                className="w-full px-3 py-1.5 bg-white border border-slate-300 rounded focus:ring-1 focus:ring-blue-500 outline-none"
              />
            </div>
          </div>
        </div>

        {/* Text Enhancers */}
        <div className="space-y-4">
          <div className="space-y-2">
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Brand Enhancer
            </label>
            <input
              type="text"
              name="BRAND_ENHANCER"
              value={formData.BRAND_ENHANCER}
              onChange={handleChange}
              className="w-full px-4 py-2 bg-slate-50 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
            />
          </div>

          <div className="space-y-2">
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Model Enhancer
            </label>
            <input
              type="text"
              name="MODEL_ENHANCER"
              value={formData.MODEL_ENHANCER}
              onChange={handleChange}
              className="w-full px-4 py-2 bg-slate-50 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
            />
          </div>
        </div>

        {/* Season and Limits */}
        <div className="space-y-4">
          <div className="space-y-2">
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Season
            </label>
            <select
              name="SEASON"
              value={formData.SEASON}
              onChange={handleChange}
              className="w-full px-4 py-2 bg-slate-50 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none appearance-none cursor-pointer"
            >
              <option value={Season.AllSeason}>All Season</option>
              <option value={Season.Winter}>Winter</option>
              <option value={Season.Summer}>Summer</option>
              <option value=""></option>
            </select>
            <p className="text-xs text-slate-400">Leave blank to process full dataset.</p>
          </div>

          <div className="space-y-2">
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Limit to Segments (Optional)
            </label>
            <input
              type="text"
              name="LIMIT_TO_SEGMENTS"
              value={formData.LIMIT_TO_SEGMENTS}
              onChange={handleChange}
              placeholder="Run all segments"
              className="w-full px-4 py-2 bg-slate-50 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
            />
            <p className="text-xs text-slate-400">Leave blank to process full dataset.</p>
          </div>
        </div>

      </form>

      <div className="p-6 bg-slate-50 border-t border-slate-200 flex gap-3">
        <button
          type="button"
          onClick={handleReset}
          disabled={isLoading}
          className="px-4 py-2 text-slate-600 bg-white border border-slate-300 rounded-lg hover:bg-slate-50 transition-colors flex items-center gap-2 disabled:opacity-50"
        >
          <RotateCcw size={18} />
          Reset
        </button>

        {isLoading ? (
          <button
            onClick={handleReload}
            className="flex-1 px-4 py-2 text-white bg-red-500 rounded-lg hover:bg-red-600 transition-all flex items-center justify-center gap-2 shadow-md"
          >
            <XCircle size={18} />
            Cancel & Reload
          </button>
        ) : (
          <button
            onClick={handleSubmit}
            className={`flex-1 px-4 py-2 text-white bg-[#00ab04] rounded-lg hover:bg-[#008003] transition-all flex items-center justify-center gap-2 shadow-md hover:shadow-lg`}
          >
            <Save size={18} />
            Save Configuration
          </button>
        )}
      </div>
    </div>
  );
};
