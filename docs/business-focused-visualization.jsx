import React, { useState, useEffect } from 'react';

const EnhancedFlatteningVisualization = () => {
  const [activeSection, setActiveSection] = useState(0);
  const [selectedExample, setSelectedExample] = useState('ecommerce');
  const [showCorruption, setShowCorruption] = useState(false);
  const [animationPhase, setAnimationPhase] = useState(0);

  useEffect(() => {
    if (showCorruption) {
      const timer = setInterval(() => {
        setAnimationPhase(prev => (prev < 4 ? prev + 1 : 4));
      }, 600);
      return () => clearInterval(timer);
    } else {
      setAnimationPhase(0);
    }
  }, [showCorruption]);

  const sections = [
    { id: 0, title: "Real-World Impact", icon: "🌍" },
    { id: 1, title: "The Comma Crisis", icon: "💥" },
    { id: 2, title: "Bracket Anatomy", icon: "🔬" },
    { id: 3, title: "Live Corruption Demo", icon: "⚡" },
    { id: 4, title: "Business Solutions", icon: "✨" }
  ];

  // Real-world examples data
  const realWorldExamples = {
    ecommerce: {
      title: "E-Commerce Order System",
      icon: "🛒",
      color: "from-violet-500 to-fuchsia-500",
      bgColor: "violet",
      original: {
        description: "Customer order with multiple items, each having product variants",
        data: `{
  "order_id": "ORD-2024-7891",
  "customer": "Acme Corp",
  "items": [
    {
      "product": "Laptop Pro 15",
      "variants": [
        {"size": "15-inch", "color": "Space Gray"},
        {"size": "15-inch", "color": "Silver"}
      ]
    },
    {
      "product": "USB-C Hub",
      "variants": [
        {"ports": "7-in-1"}
      ]
    }
  ]
}`
      },
      flattened: {
        correct: `items_variants_color: "[[Space Gray, Silver], [null]]"`,
        broken: `items_variants_color: "[Space Gray, Silver, null]"`,
        issue: "Lost: Which colors belong to which product"
      },
      businessImpact: "Warehouse ships wrong color to customer → Returns increase 340%"
    },
    healthcare: {
      title: "Patient Medical Records",
      icon: "🏥",
      color: "from-emerald-500 to-teal-500",
      bgColor: "emerald",
      original: {
        description: "Patient with multiple visits, each with multiple prescriptions",
        data: `{
  "patient_id": "P-445521",
  "name": "Jane Doe",
  "visits": [
    {
      "date": "2024-01-15",
      "prescriptions": [
        {"drug": "Lisinopril, 10mg", "dosage": "1x daily"},
        {"drug": "Metformin", "dosage": "2x daily"}
      ]
    },
    {
      "date": "2024-03-22",
      "prescriptions": [
        {"drug": "Aspirin, 81mg", "dosage": "1x daily"}
      ]
    }
  ]
}`
      },
      flattened: {
        correct: `visits_prescriptions_drug: "[[\\"Lisinopril, 10mg\\", Metformin], [\\"Aspirin, 81mg\\"]]"`,
        broken: `visits_prescriptions_drug: "[Lisinopril, 10mg, Metformin, Aspirin, 81mg]"`,
        issue: "Lost: Which prescriptions from which visit + comma in drug name splits incorrectly"
      },
      businessImpact: "System shows 5 prescriptions instead of 3 → Potential dosing errors"
    },
    finance: {
      title: "Investment Portfolio",
      icon: "📈",
      color: "from-amber-500 to-orange-500",
      bgColor: "amber",
      original: {
        description: "Client portfolio with multiple accounts, each with holdings",
        data: `{
  "client_id": "CL-99821",
  "accounts": [
    {
      "type": "Retirement 401(k)",
      "holdings": [
        {"ticker": "AAPL", "shares": 150},
        {"ticker": "MSFT", "shares": 200}
      ]
    },
    {
      "type": "Brokerage",
      "holdings": [
        {"ticker": "GOOGL", "shares": 50}
      ]
    }
  ]
}`
      },
      flattened: {
        correct: `accounts_holdings_ticker: "[[AAPL, MSFT], [GOOGL]]"`,
        broken: `accounts_holdings_ticker: "[AAPL, MSFT, GOOGL]"`,
        issue: "Lost: Which holdings are in retirement vs taxable account"
      },
      businessImpact: "Tax reporting shows wrong account allocations → IRS compliance issues"
    },
    logistics: {
      title: "Shipping & Logistics",
      icon: "🚚",
      color: "from-sky-500 to-blue-500",
      bgColor: "sky",
      original: {
        description: "Shipment with multiple containers, each with package manifests",
        data: `{
  "shipment_id": "SHP-2024-4421",
  "containers": [
    {
      "container_id": "CONT-A",
      "packages": [
        {"tracking": "1Z999AA1", "weight": "5.2 lbs"},
        {"tracking": "1Z999AA2", "weight": "12.8 lbs"}
      ]
    },
    {
      "container_id": "CONT-B",
      "packages": [
        {"tracking": "1Z999BB1", "weight": "3.1 lbs"}
      ]
    }
  ]
}`
      },
      flattened: {
        correct: `containers_packages_weight: "[[\\"5.2 lbs\\", \\"12.8 lbs\\"], [\\"3.1 lbs\\"]]"`,
        broken: `containers_packages_weight: "[5.2 lbs, 12.8 lbs, 3.1 lbs]"`,
        issue: "Lost: Which packages in which container + weight units have decimals"
      },
      businessImpact: "Container weight calculations wrong → Shipping overcharges or safety violations"
    }
  };

  // Section 0: Real-World Impact
  const RealWorldImpact = () => {
    const example = realWorldExamples[selectedExample];
    
    return (
      <div className="space-y-8">
        {/* Example Selector */}
        <div className="flex flex-wrap gap-3 justify-center">
          {Object.entries(realWorldExamples).map(([key, ex]) => (
            <button
              key={key}
              onClick={() => setSelectedExample(key)}
              className={`px-5 py-3 rounded-2xl font-semibold transition-all duration-300 flex items-center gap-2 ${
                selectedExample === key
                  ? `bg-gradient-to-r ${ex.color} text-white shadow-lg shadow-${ex.bgColor}-500/30 scale-105`
                  : 'bg-white/10 text-white/70 hover:bg-white/20 hover:scale-102'
              }`}
            >
              <span className="text-xl">{ex.icon}</span>
              <span className="hidden sm:inline">{ex.title}</span>
            </button>
          ))}
        </div>

        {/* Main Example Card */}
        <div className={`bg-gradient-to-br ${example.color} p-1 rounded-3xl shadow-2xl shadow-${example.bgColor}-500/20`}>
          <div className="bg-slate-900/95 backdrop-blur rounded-3xl p-8">
            <div className="flex items-center gap-4 mb-6">
              <div className={`w-16 h-16 bg-gradient-to-br ${example.color} rounded-2xl flex items-center justify-center text-3xl shadow-lg`}>
                {example.icon}
              </div>
              <div>
                <h3 className="text-2xl font-bold text-white">{example.title}</h3>
                <p className="text-white/60">{example.original.description}</p>
              </div>
            </div>

            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
              {/* Original Data */}
              <div className="space-y-3">
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 rounded-full bg-emerald-400 animate-pulse"></div>
                  <span className="text-emerald-400 font-mono text-sm uppercase tracking-wider">Original Nested Structure</span>
                </div>
                <pre className="bg-black/50 rounded-2xl p-5 text-sm overflow-x-auto border border-white/10">
                  <code className="text-emerald-300">{example.original.data}</code>
                </pre>
              </div>

              {/* Flattened Comparison */}
              <div className="space-y-4">
                <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-2xl p-5">
                  <div className="flex items-center gap-2 mb-3">
                    <span className="text-2xl">✅</span>
                    <span className="text-emerald-400 font-bold">Correct (With Brackets)</span>
                  </div>
                  <code className="text-emerald-300 text-sm font-mono block bg-black/30 rounded-xl p-3">
                    {example.flattened.correct}
                  </code>
                </div>

                <div className="bg-red-500/10 border border-red-500/30 rounded-2xl p-5">
                  <div className="flex items-center gap-2 mb-3">
                    <span className="text-2xl">❌</span>
                    <span className="text-red-400 font-bold">Broken (Without Structure)</span>
                  </div>
                  <code className="text-red-300 text-sm font-mono block bg-black/30 rounded-xl p-3">
                    {example.flattened.broken}
                  </code>
                  <p className="text-red-400/80 text-sm mt-3 flex items-start gap-2">
                    <span>⚠️</span>
                    {example.flattened.issue}
                  </p>
                </div>
              </div>
            </div>

            {/* Business Impact Alert */}
            <div className={`mt-6 bg-gradient-to-r ${example.color} p-0.5 rounded-2xl`}>
              <div className="bg-slate-900 rounded-2xl p-5 flex items-center gap-4">
                <div className="text-4xl">💰</div>
                <div>
                  <div className="text-white font-bold text-lg">Business Impact</div>
                  <div className="text-white/80">{example.businessImpact}</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  };

  // Section 1: The Comma Crisis
  const CommaCollision = () => (
    <div className="space-y-8">
      <div className="bg-gradient-to-br from-rose-500 to-pink-600 p-1 rounded-3xl">
        <div className="bg-slate-900/95 rounded-3xl p-8">
          <h3 className="text-3xl font-bold text-white mb-2 flex items-center gap-3">
            <span className="text-4xl">🎭</span>
            The "Comma in Your Data" Problem
          </h3>
          <p className="text-white/60 text-lg mb-8">When your data contains the same character as your delimiter, chaos ensues.</p>

          {/* Interactive Demo */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
            {/* Input */}
            <div className="bg-gradient-to-br from-emerald-500/20 to-teal-500/20 rounded-2xl p-6 border border-emerald-500/30">
              <div className="text-emerald-400 font-mono text-xs uppercase tracking-wider mb-3">Original Array</div>
              <div className="text-2xl font-mono text-white mb-4">
                ["Smith, John", "Doe"]
              </div>
              <div className="flex gap-2">
                <span className="px-3 py-1 bg-emerald-500/20 rounded-full text-emerald-300 text-sm">2 elements</span>
                <span className="px-3 py-1 bg-emerald-500/20 rounded-full text-emerald-300 text-sm">Clear meaning</span>
              </div>
            </div>

            {/* Arrow */}
            <div className="flex items-center justify-center">
              <div className="text-6xl animate-bounce text-rose-400">→</div>
            </div>

            {/* Output */}
            <div className="bg-gradient-to-br from-rose-500/20 to-pink-500/20 rounded-2xl p-6 border border-rose-500/30">
              <div className="text-rose-400 font-mono text-xs uppercase tracking-wider mb-3">Comma-Separated Output</div>
              <div className="text-2xl font-mono text-white mb-4">
                Smith, John, Doe
              </div>
              <div className="flex gap-2">
                <span className="px-3 py-1 bg-rose-500/20 rounded-full text-rose-300 text-sm">2 or 3 elements?</span>
                <span className="px-3 py-1 bg-rose-500/20 rounded-full text-rose-300 text-sm">🤷 Unknown</span>
              </div>
            </div>
          </div>

          {/* Possible Interpretations */}
          <div className="bg-black/30 rounded-2xl p-6 mb-6">
            <h4 className="text-xl font-bold text-rose-400 mb-4">Given "Smith, John, Doe" — Which was the original?</h4>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              {[
                { arr: '["Smith, John", "Doe"]', count: 2, prob: "Maybe?" },
                { arr: '["Smith", "John", "Doe"]', count: 3, prob: "Maybe?" },
                { arr: '["Smith, John, Doe"]', count: 1, prob: "Maybe?" },
                { arr: '["Smith", "John, Doe"]', count: 2, prob: "Maybe?" }
              ].map((opt, idx) => (
                <div key={idx} className="bg-white/5 rounded-xl p-4 border border-white/10 hover:border-rose-500/50 transition-all hover:scale-105 cursor-pointer">
                  <div className="font-mono text-sm text-white/90 mb-2">{opt.arr}</div>
                  <div className="flex justify-between items-center">
                    <span className="text-rose-400 text-xs">{opt.count} element(s)</span>
                    <span className="text-amber-400 text-xs">{opt.prob}</span>
                  </div>
                </div>
              ))}
            </div>
            <div className="text-center mt-6">
              <span className="text-5xl">🤷</span>
              <p className="text-rose-400 font-bold text-xl mt-2">ALL are valid interpretations!</p>
              <p className="text-white/60">The data is now UNRECOVERABLE</p>
            </div>
          </div>

          {/* Real Examples Table */}
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-white/10">
                  <th className="text-left py-3 px-4 text-rose-400 font-semibold">Real Data Scenario</th>
                  <th className="text-left py-3 px-4 text-rose-400 font-semibold">Original</th>
                  <th className="text-left py-3 px-4 text-rose-400 font-semibold">After Comma-Sep</th>
                  <th className="text-left py-3 px-4 text-rose-400 font-semibold">Problem</th>
                </tr>
              </thead>
              <tbody className="text-white/80">
                <tr className="border-b border-white/5 hover:bg-white/5">
                  <td className="py-3 px-4">💵 Currency</td>
                  <td className="py-3 px-4 font-mono text-sm">["$1,234.56", "$789.00"]</td>
                  <td className="py-3 px-4 font-mono text-sm text-rose-300">$1,234.56,$789.00</td>
                  <td className="py-3 px-4 text-amber-400">3 values or 2?</td>
                </tr>
                <tr className="border-b border-white/5 hover:bg-white/5">
                  <td className="py-3 px-4">📍 Addresses</td>
                  <td className="py-3 px-4 font-mono text-sm">["123 Main, NYC", "456 Oak"]</td>
                  <td className="py-3 px-4 font-mono text-sm text-rose-300">123 Main, NYC, 456 Oak</td>
                  <td className="py-3 px-4 text-amber-400">3 values or 2?</td>
                </tr>
                <tr className="border-b border-white/5 hover:bg-white/5">
                  <td className="py-3 px-4">📅 Dates</td>
                  <td className="py-3 px-4 font-mono text-sm">["Jan 1, 2024", "Feb 15, 2024"]</td>
                  <td className="py-3 px-4 font-mono text-sm text-rose-300">Jan 1, 2024, Feb 15, 2024</td>
                  <td className="py-3 px-4 text-amber-400">4 values or 2?</td>
                </tr>
                <tr className="hover:bg-white/5">
                  <td className="py-3 px-4">🏢 Companies</td>
                  <td className="py-3 px-4 font-mono text-sm">["Acme, Inc.", "Beta LLC"]</td>
                  <td className="py-3 px-4 font-mono text-sm text-rose-300">Acme, Inc., Beta LLC</td>
                  <td className="py-3 px-4 text-amber-400">3 values or 2?</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );

  // Section 2: Bracket Anatomy
  const BracketAnatomy = () => (
    <div className="space-y-8">
      <div className="bg-gradient-to-br from-cyan-500 to-blue-600 p-1 rounded-3xl">
        <div className="bg-slate-900/95 rounded-3xl p-8">
          <h3 className="text-3xl font-bold text-white mb-6 flex items-center gap-3">
            <span className="text-4xl">🔬</span>
            Anatomy of Nested Brackets
          </h3>

          {/* Visual Tree Diagram */}
          <div className="bg-black/30 rounded-2xl p-8 mb-8">
            <div className="text-center text-white/60 text-sm mb-6 font-mono uppercase tracking-wider">
              E-Commerce Order Hierarchy
            </div>
            
            <div className="flex flex-col items-center space-y-4">
              {/* Order */}
              <div className="bg-gradient-to-r from-violet-500 to-purple-600 px-8 py-4 rounded-2xl shadow-lg shadow-violet-500/30">
                <span className="text-white font-bold text-lg">📦 Order #12345</span>
              </div>
              
              <div className="w-1 h-8 bg-gradient-to-b from-violet-500 to-cyan-500"></div>
              
              {/* Products Array */}
              <div className="bg-gradient-to-r from-cyan-500 to-blue-500 px-6 py-3 rounded-xl shadow-lg shadow-cyan-500/30">
                <span className="text-white font-semibold">products[ ]</span>
              </div>
              
              <div className="flex gap-32">
                <div className="w-1 h-8 bg-cyan-500"></div>
                <div className="w-1 h-8 bg-cyan-500"></div>
              </div>
              
              {/* Products */}
              <div className="flex gap-8">
                <div className="flex flex-col items-center">
                  <div className="bg-gradient-to-r from-amber-500 to-orange-500 px-5 py-3 rounded-xl shadow-lg shadow-amber-500/30 mb-4">
                    <span className="text-white font-semibold">🖥️ Laptop Pro</span>
                  </div>
                  <div className="bg-amber-500/20 border border-amber-500/30 px-4 py-2 rounded-lg mb-2">
                    <span className="text-amber-300 text-sm">variants[ ]</span>
                  </div>
                  <div className="flex gap-2">
                    <span className="px-3 py-1 bg-amber-500/10 rounded-lg text-amber-200 text-xs border border-amber-500/20">Space Gray</span>
                    <span className="px-3 py-1 bg-amber-500/10 rounded-lg text-amber-200 text-xs border border-amber-500/20">Silver</span>
                    <span className="px-3 py-1 bg-amber-500/10 rounded-lg text-amber-200 text-xs border border-amber-500/20">Gold</span>
                  </div>
                </div>
                
                <div className="flex flex-col items-center">
                  <div className="bg-gradient-to-r from-emerald-500 to-teal-500 px-5 py-3 rounded-xl shadow-lg shadow-emerald-500/30 mb-4">
                    <span className="text-white font-semibold">🔌 USB Hub</span>
                  </div>
                  <div className="bg-emerald-500/20 border border-emerald-500/30 px-4 py-2 rounded-lg mb-2">
                    <span className="text-emerald-300 text-sm">variants[ ]</span>
                  </div>
                  <div className="flex gap-2">
                    <span className="px-3 py-1 bg-emerald-500/10 rounded-lg text-emerald-200 text-xs border border-emerald-500/20">Black</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Flattened Output Explanation */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-2xl p-6">
              <h4 className="text-emerald-400 font-bold text-xl mb-4 flex items-center gap-2">
                <span>✅</span> Correct Flattened Format
              </h4>
              <div className="bg-black/30 rounded-xl p-4 font-mono text-sm mb-4">
                <span className="text-white/60">products_variants_color:</span>
                <div className="text-lg mt-2">
                  <span className="text-violet-400 font-bold">[</span>
                  <span className="text-amber-400">[Space Gray, Silver, Gold]</span>
                  <span className="text-white/40">, </span>
                  <span className="text-emerald-400">[Black]</span>
                  <span className="text-violet-400 font-bold">]</span>
                </div>
              </div>
              <div className="space-y-2 text-sm">
                <div className="flex items-center gap-2">
                  <span className="w-4 h-4 rounded bg-violet-500"></span>
                  <span className="text-white/80">Outer [ ] = products array (2 products)</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-4 h-4 rounded bg-amber-500"></span>
                  <span className="text-white/80">First inner = Laptop's 3 variants</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-4 h-4 rounded bg-emerald-500"></span>
                  <span className="text-white/80">Second inner = USB Hub's 1 variant</span>
                </div>
              </div>
            </div>

            <div className="bg-rose-500/10 border border-rose-500/30 rounded-2xl p-6">
              <h4 className="text-rose-400 font-bold text-xl mb-4 flex items-center gap-2">
                <span>❌</span> If Outer Brackets Removed
              </h4>
              <div className="bg-black/30 rounded-xl p-4 font-mono text-sm mb-4">
                <span className="text-white/60">products_variants_color:</span>
                <div className="text-lg mt-2 text-rose-400">
                  [Space Gray, Silver, Gold, Black]
                </div>
              </div>
              <div className="space-y-2 text-sm text-rose-300">
                <div className="flex items-center gap-2">
                  <span>❌</span>
                  <span>Lost: Which colors belong to which product</span>
                </div>
                <div className="flex items-center gap-2">
                  <span>❌</span>
                  <span>Lost: Laptop had 3 options, Hub had 1</span>
                </div>
                <div className="flex items-center gap-2">
                  <span>❌</span>
                  <span>System thinks: 1 product with 4 colors</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );

  // Section 3: Live Corruption Demo
  const LiveCorruptionDemo = () => (
    <div className="space-y-8">
      <div className="bg-gradient-to-br from-rose-600 to-red-700 p-1 rounded-3xl">
        <div className="bg-slate-900/95 rounded-3xl p-8">
          <h3 className="text-3xl font-bold text-white mb-2 flex items-center gap-3">
            <span className="text-4xl">⚡</span>
            Watch Data Corruption Happen
          </h3>
          <p className="text-white/60 text-lg mb-8">Click the button to see how removing brackets destroys your data in real-time.</p>

          <button
            onClick={() => setShowCorruption(!showCorruption)}
            className={`px-8 py-4 rounded-2xl font-bold text-lg transition-all duration-300 ${
              showCorruption
                ? 'bg-emerald-500 hover:bg-emerald-600 text-white'
                : 'bg-gradient-to-r from-rose-500 to-red-600 hover:from-rose-600 hover:to-red-700 text-white shadow-lg shadow-rose-500/30'
            }`}
          >
            {showCorruption ? '🔄 Reset Demo' : '💥 Trigger Data Corruption'}
          </button>

          {/* Animation Sequence */}
          <div className="mt-8 space-y-6">
            {/* Step 1: Original */}
            <div className={`transition-all duration-500 ${animationPhase >= 0 ? 'opacity-100' : 'opacity-30'}`}>
              <div className="flex items-center gap-3 mb-3">
                <div className={`w-8 h-8 rounded-full flex items-center justify-center font-bold ${animationPhase >= 0 ? 'bg-emerald-500 text-white' : 'bg-white/20 text-white/50'}`}>1</div>
                <span className="text-emerald-400 font-semibold">Original Hospital Data</span>
              </div>
              <div className="bg-black/30 rounded-xl p-4 font-mono text-sm">
                <div className="text-white/60 mb-2">// Patient visits with prescriptions</div>
                <div className="text-emerald-300">visits_prescriptions_drug: <span className="text-amber-300">"[[Aspirin, 81mg, Metformin], [Lisinopril]]"</span></div>
                <div className="text-white/40 text-xs mt-2">✓ Visit 1: 2 drugs (one has comma in name) | Visit 2: 1 drug</div>
              </div>
            </div>

            {/* Step 2: Business Request */}
            <div className={`transition-all duration-500 ${animationPhase >= 1 ? 'opacity-100' : 'opacity-30'}`}>
              <div className="flex items-center gap-3 mb-3">
                <div className={`w-8 h-8 rounded-full flex items-center justify-center font-bold ${animationPhase >= 1 ? 'bg-amber-500 text-white' : 'bg-white/20 text-white/50'}`}>2</div>
                <span className="text-amber-400 font-semibold">Business Request: "Remove those extra brackets"</span>
              </div>
              <div className="bg-amber-500/10 border border-amber-500/30 rounded-xl p-4">
                <div className="text-amber-300 italic">"The brackets look redundant. Can we simplify to just one level?"</div>
              </div>
            </div>

            {/* Step 3: Simplified */}
            <div className={`transition-all duration-500 ${animationPhase >= 2 ? 'opacity-100' : 'opacity-30'}`}>
              <div className="flex items-center gap-3 mb-3">
                <div className={`w-8 h-8 rounded-full flex items-center justify-center font-bold ${animationPhase >= 2 ? 'bg-rose-500 text-white' : 'bg-white/20 text-white/50'}`}>3</div>
                <span className="text-rose-400 font-semibold">"Simplified" Output</span>
              </div>
              <div className="bg-black/30 rounded-xl p-4 font-mono text-sm">
                <div className="text-rose-300">visits_prescriptions_drug: <span className="text-rose-400">"[Aspirin, 81mg, Metformin, Lisinopril]"</span></div>
                <div className="text-rose-400/60 text-xs mt-2">⚠️ Now it looks like 4 separate drugs!</div>
              </div>
            </div>

            {/* Step 4: Reconstruction Attempt */}
            <div className={`transition-all duration-500 ${animationPhase >= 3 ? 'opacity-100' : 'opacity-30'}`}>
              <div className="flex items-center gap-3 mb-3">
                <div className={`w-8 h-8 rounded-full flex items-center justify-center font-bold ${animationPhase >= 3 ? 'bg-red-600 text-white animate-pulse' : 'bg-white/20 text-white/50'}`}>4</div>
                <span className="text-red-400 font-semibold">System Tries to Reconstruct...</span>
              </div>
              <div className="bg-red-500/10 border-2 border-red-500/50 rounded-xl p-6">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div>
                    <div className="text-white/60 text-sm mb-2">Expected:</div>
                    <div className="bg-black/30 rounded-lg p-3 text-sm">
                      <div className="text-emerald-300">Visit 1: ["Aspirin, 81mg", "Metformin"]</div>
                      <div className="text-emerald-300">Visit 2: ["Lisinopril"]</div>
                    </div>
                  </div>
                  <div>
                    <div className="text-white/60 text-sm mb-2">Actual (WRONG):</div>
                    <div className="bg-black/30 rounded-lg p-3 text-sm">
                      <div className="text-red-400">Visit 1: ["Aspirin", "81mg", "Metformin", "Lisinopril"] ???</div>
                      <div className="text-red-400/50">Visit 2: (no data)</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Step 5: Consequence */}
            {animationPhase >= 4 && (
              <div className="animate-pulse bg-gradient-to-r from-red-600 to-rose-600 rounded-2xl p-6 text-center">
                <div className="text-5xl mb-4">🚨</div>
                <div className="text-white text-2xl font-bold mb-2">PATIENT SAFETY ALERT</div>
                <div className="text-white/90">
                  System shows patient taking "81mg" as a separate medication<br/>
                  Prescriptions from Visit 2 are now orphaned<br/>
                  <span className="font-bold">Medical records are CORRUPTED</span>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );

  // Section 4: Business Solutions
  const BusinessSolutions = () => (
    <div className="space-y-8">
      <div className="bg-gradient-to-br from-emerald-500 to-teal-600 p-1 rounded-3xl">
        <div className="bg-slate-900/95 rounded-3xl p-8">
          <h3 className="text-3xl font-bold text-white mb-6 flex items-center gap-3">
            <span className="text-4xl">✨</span>
            Solutions That Actually Work
          </h3>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Recommended */}
            <div className="bg-gradient-to-br from-emerald-500/20 to-teal-500/20 border-2 border-emerald-500/50 rounded-2xl p-6 relative overflow-hidden">
              <div className="absolute top-0 right-0 bg-emerald-500 text-white px-4 py-1 rounded-bl-xl font-bold text-sm">
                ⭐ RECOMMENDED
              </div>
              <h4 className="text-emerald-400 font-bold text-xl mt-4 mb-3">Keep Full Bracket Structure</h4>
              <div className="bg-black/30 rounded-xl p-4 font-mono text-sm mb-4">
                <div className="text-emerald-300">[[value1, value2], [value3]]</div>
              </div>
              <div className="space-y-2 text-sm">
                <div className="flex items-center gap-2 text-emerald-300">
                  <span>✅</span> 100% reconstructable
                </div>
                <div className="flex items-center gap-2 text-emerald-300">
                  <span>✅</span> Handles ANY data content
                </div>
                <div className="flex items-center gap-2 text-emerald-300">
                  <span>✅</span> Zero data loss risk
                </div>
                <div className="flex items-center gap-2 text-emerald-300">
                  <span>✅</span> Works in all edge cases
                </div>
              </div>
              <div className="mt-4 p-3 bg-emerald-500/10 rounded-xl">
                <div className="text-emerald-400 text-sm font-medium">💡 Business Talking Point:</div>
                <div className="text-white/70 text-sm">"The brackets are metadata that tells us which items belong together. Removing them is like removing commas from a sentence."</div>
              </div>
            </div>

            {/* JSON Alternative */}
            <div className="bg-white/5 border border-white/10 rounded-2xl p-6">
              <div className="text-cyan-400 text-sm font-semibold mb-2">ALTERNATIVE</div>
              <h4 className="text-white font-bold text-xl mb-3">JSON Format with Quotes</h4>
              <div className="bg-black/30 rounded-xl p-4 font-mono text-sm mb-4">
                <div className="text-cyan-300">[["Smith, John", "Doe"], ["Jane"]]</div>
              </div>
              <div className="space-y-2 text-sm">
                <div className="flex items-center gap-2 text-cyan-300">
                  <span>✅</span> Handles commas in data
                </div>
                <div className="flex items-center gap-2 text-cyan-300">
                  <span>✅</span> Industry standard
                </div>
                <div className="flex items-center gap-2 text-amber-300">
                  <span>⚠️</span> Business sees "extra quotes"
                </div>
              </div>
            </div>

            {/* Pipe Delimiter */}
            <div className="bg-white/5 border border-white/10 rounded-2xl p-6">
              <div className="text-amber-400 text-sm font-semibold mb-2">PARTIAL FIX</div>
              <h4 className="text-white font-bold text-xl mb-3">Alternative Delimiter (Pipe)</h4>
              <div className="bg-black/30 rounded-xl p-4 font-mono text-sm mb-4">
                <div className="text-amber-300">value1|value2|value3</div>
              </div>
              <div className="space-y-2 text-sm">
                <div className="flex items-center gap-2 text-amber-300">
                  <span>⚠️</span> Works IF data has no pipes
                </div>
                <div className="flex items-center gap-2 text-rose-300">
                  <span>❌</span> Still loses nesting structure
                </div>
                <div className="flex items-center gap-2 text-rose-300">
                  <span>❌</span> Just shifts the problem
                </div>
              </div>
            </div>

            {/* What NOT to do */}
            <div className="bg-rose-500/10 border border-rose-500/30 rounded-2xl p-6">
              <div className="text-rose-400 text-sm font-semibold mb-2">❌ AVOID</div>
              <h4 className="text-white font-bold text-xl mb-3">Remove Brackets / Flatten</h4>
              <div className="bg-black/30 rounded-xl p-4 font-mono text-sm mb-4">
                <div className="text-rose-300 line-through">value1, value2, value3</div>
              </div>
              <div className="space-y-2 text-sm text-rose-300">
                <div className="flex items-center gap-2">
                  <span>❌</span> Data becomes unrecoverable
                </div>
                <div className="flex items-center gap-2">
                  <span>❌</span> Comma collision inevitable
                </div>
                <div className="flex items-center gap-2">
                  <span>❌</span> Structure information lost
                </div>
              </div>
            </div>
          </div>

          {/* Final Message */}
          <div className="mt-8 bg-gradient-to-r from-violet-500 via-fuchsia-500 to-pink-500 p-1 rounded-2xl">
            <div className="bg-slate-900 rounded-2xl p-6">
              <div className="text-center">
                <div className="text-4xl mb-4">🎯</div>
                <h4 className="text-2xl font-bold text-white mb-3">The Bottom Line</h4>
                <p className="text-white/80 text-lg max-w-2xl mx-auto">
                  The brackets aren't "extra" — they're <span className="text-amber-400 font-bold">essential metadata</span> that preserves the relationship between data elements.
                  Removing them is like removing the commas from a sentence and expecting it to still make sense.
                </p>
                <div className="mt-6 inline-flex items-center gap-2 bg-emerald-500/20 px-6 py-3 rounded-full">
                  <span className="text-emerald-400 font-bold">✅ Recommendation:</span>
                  <span className="text-white">Keep BRACKET_LIST format with full nesting</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
      {/* Animated Background */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-0 left-1/4 w-96 h-96 bg-violet-500/10 rounded-full blur-3xl animate-pulse"></div>
        <div className="absolute bottom-0 right-1/4 w-96 h-96 bg-cyan-500/10 rounded-full blur-3xl animate-pulse" style={{animationDelay: '1s'}}></div>
        <div className="absolute top-1/2 left-1/2 w-96 h-96 bg-fuchsia-500/5 rounded-full blur-3xl animate-pulse" style={{animationDelay: '2s'}}></div>
      </div>

      {/* Header */}
      <header className="relative border-b border-white/10 bg-black/20 backdrop-blur-xl">
        <div className="max-w-7xl mx-auto px-6 py-8">
          <div className="flex items-center gap-5">
            <div className="w-16 h-16 bg-gradient-to-br from-amber-400 via-orange-500 to-rose-500 rounded-2xl flex items-center justify-center text-3xl shadow-2xl shadow-orange-500/30 animate-pulse">
              📊
            </div>
            <div>
              <h1 className="text-4xl font-black bg-gradient-to-r from-amber-400 via-orange-400 to-rose-400 bg-clip-text text-transparent">
                Array Flattening Explained
              </h1>
              <p className="text-white/60 text-lg">Why bracket preservation prevents data corruption</p>
            </div>
          </div>

          {/* Navigation */}
          <nav className="flex gap-2 mt-8 flex-wrap">
            {sections.map((section) => (
              <button
                key={section.id}
                onClick={() => setActiveSection(section.id)}
                className={`px-5 py-3 rounded-xl font-semibold transition-all duration-300 flex items-center gap-2 ${
                  activeSection === section.id
                    ? 'bg-gradient-to-r from-amber-500 to-orange-500 text-white shadow-lg shadow-orange-500/30 scale-105'
                    : 'bg-white/5 text-white/60 hover:bg-white/10 hover:text-white/90'
                }`}
              >
                <span className="text-xl">{section.icon}</span>
                <span className="hidden md:inline">{section.title}</span>
              </button>
            ))}
          </nav>
        </div>
      </header>

      {/* Main Content */}
      <main className="relative max-w-7xl mx-auto px-6 py-10">
        {activeSection === 0 && <RealWorldImpact />}
        {activeSection === 1 && <CommaCollision />}
        {activeSection === 2 && <BracketAnatomy />}
        {activeSection === 3 && <LiveCorruptionDemo />}
        {activeSection === 4 && <BusinessSolutions />}
      </main>

      {/* Footer */}
      <footer className="relative border-t border-white/10 bg-black/20 backdrop-blur-xl mt-12">
        <div className="max-w-7xl mx-auto px-6 py-6">
          <div className="flex flex-wrap justify-between items-center gap-4 text-white/40 text-sm">
            <div className="flex items-center gap-2">
              <span className="text-amber-400 font-mono">io.github.pierce</span>
              <span>•</span>
              <span>MapFlattener & AvroReconstructor</span>
            </div>
            <div className="flex gap-4">
              <span className="px-3 py-1 bg-white/5 rounded-lg">JSON</span>
              <span className="px-3 py-1 bg-white/5 rounded-lg">BRACKET_LIST</span>
              <span className="px-3 py-1 bg-white/5 rounded-lg">PIPE_SEPARATED</span>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default EnhancedFlatteningVisualization;
