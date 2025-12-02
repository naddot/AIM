import React from 'react';

export const HeroSection: React.FC = () => {
  return (
    <section className="w-full bg-bc-green text-white pb-24 pt-16 px-4 brand-ramp shadow-lg relative overflow-hidden">
      <div className="absolute inset-0 bg-gradient-to-br from-[#00ab04] via-emerald-500 to-emerald-700" aria-hidden="true" />
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_20%_20%,rgba(255,255,255,0.12),transparent_45%),radial-gradient(circle_at_80%_0%,rgba(255,255,255,0.08),transparent_35%)]" aria-hidden="true" />
      <div className="absolute inset-x-0 -bottom-14 h-24 bg-white/10 blur-xl -skew-y-3 origin-top-left" aria-hidden="true" />

      <div className="max-w-3xl mx-auto text-center relative z-10">
        <h1 className="text-4xl md:text-6xl font-extrabold mb-6 tracking-tight leading-tight">
          The simple way to
          <br />
          run AIM
        </h1>
        <p className="text-xl md:text-2xl font-medium opacity-90 max-w-2xl mx-auto">
          Choose your settings, sync config, receive results.
        </p>
      </div>

      <div className="absolute -right-20 -top-20 w-64 h-64 border-[20px] border-white/10 rounded-full pointer-events-none" aria-hidden="true"></div>
    </section>
  );
};
