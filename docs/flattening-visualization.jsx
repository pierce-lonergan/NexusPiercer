import React, { useState, useEffect } from 'react';

const FlatteningVisualization = () => {
  const [activeSection, setActiveSection] = useState(0);
  const [showDataLoss, setShowDataLoss] = useState(false);
  const [animationStep, setAnimationStep] = useState(0);
  const [selectedScenario, setSelectedScenario] = useState('basic');

  useEffect(() => {
    if (showDataLoss) {
      const timer = setTimeout(() => {
        setAnimationStep(prev => (prev < 3 ? prev + 1 : 3));
      }, 800);
      return () => clearTimeout(timer);
    }
  }, [showDataLoss, animationStep]);

  const sections = [
    { id: 0, title: "Why Brackets Matter", icon: "🔐" },
    { id: 1, title: "Delimiter Collision", icon: "💥" },
    { id: 2, title: "Nested Array Structure", icon: "📦" },
    { id: 3, title: "Business Simplification", icon: "⚠️" },
    { id: 4, title: "Solutions & Trade-offs", icon: "✅" }
  ];

  // Section 0: Why Brackets Matter
  const WhyBracketsMatter = () => (
    <div className="space-y-8">
      <div className="bg-slate-900 rounded-2xl p-8 border border-slate-700">
        <h3 className="text-2xl font-bold text-amber-400 mb-6 flex items-center gap-3">
          <span className="text-3xl">🎯</span>
          The Core Problem: Array Boundary Preservation
        </h3>
        
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Original Data */}
          <div className="space-y-4">
            <div className="text-emerald-400 font-mono text-sm uppercase tracking-wider">Original Nested Data</div>
            <div className="bg-slate-950 rounded-xl p-6 font-mono text-sm border border-slate-700">
              <div className="text-slate-400">// Products with attributes</div>
              <div className="text-slate-300 mt-2">{'{'}</div>
              <div className="pl-4 text-slate-300">"products": [</div>
              <div className="pl-8 text-emerald-400">{'{ "attributes": ['}</div>
              <div className="pl-12 text-amber-400">{'{ "name": "RAM" },'}</div>
              <div className="pl-12 text-amber-400">{'{ "name": "Storage" }'}</div>
              <div className="pl-8 text-emerald-400">{']}'}</div>
              <div className="pl-8 text-cyan-400">{'{ "attributes": ['}</div>
              <div className="pl-12 text-pink-400">{'{ "name": "Bluetooth" }'}</div>
              <div className="pl-8 text-cyan-400">{']}'}</div>
              <div className="pl-4 text-slate-300">]</div>
              <div className="text-slate-300">{'}'}</div>
            </div>
          </div>

          {/* Flattened Output */}
          <div className="space-y-4">
            <div className="text-cyan-400 font-mono text-sm uppercase tracking-wider">Flattened Output</div>
            <div className="bg-slate-950 rounded-xl p-6 font-mono text-sm border border-slate-700">
              <div className="text-slate-400">// After MapFlattener</div>
              <div className="text-slate-300 mt-2">{'{'}</div>
              <div className="pl-4">
                <span className="text-emerald-400">"products_attributes_name"</span>
                <span className="text-slate-500">:</span>
              </div>
              <div className="pl-8 text-amber-300 bg-amber-950/40 rounded px-2 py-1 border border-amber-700">
                "[[RAM, Storage], [Bluetooth]]"
              </div>
              <div className="text-slate-300 mt-2">{'}'}</div>
              <div className="mt-6 pt-4 border-t border-slate-700">
                <div className="text-slate-400 text-xs">Structure encodes:</div>
                <div className="flex gap-2 mt-2">
                  <span className="px-2 py-1 bg-emerald-900/50 rounded text-emerald-400 text-xs">Product 0: 2 attrs</span>
                  <span className="px-2 py-1 bg-cyan-900/50 rounded text-cyan-400 text-xs">Product 1: 1 attr</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Key Insight Box */}
        <div className="mt-8 bg-gradient-to-r from-amber-900/30 to-orange-900/30 rounded-xl p-6 border border-amber-700/50">
          <div className="flex items-start gap-4">
            <div className="text-4xl">💡</div>
            <div>
              <h4 className="text-xl font-bold text-amber-400 mb-2">Key Insight</h4>
              <p className="text-slate-300 leading-relaxed">
                The <span className="text-amber-400 font-mono">[[...], [...]]</span> structure is <strong>not redundant</strong>. 
                It encodes which values belong to which parent object. Without it, we cannot reconstruct 
                the original hierarchy — we'd lose the mapping between attributes and their products.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );

  // Section 1: Delimiter Collision
  const DelimiterCollision = () => (
    <div className="space-y-8">
      <div className="bg-slate-900 rounded-2xl p-8 border border-slate-700">
        <h3 className="text-2xl font-bold text-red-400 mb-6 flex items-center gap-3">
          <span className="text-3xl">💥</span>
          The Delimiter Collision Problem
        </h3>

        <div className="grid grid-cols-1 gap-6">
          {/* Scenario selector */}
          <div className="flex gap-3 mb-4">
            {['basic', 'comma-in-data', 'ambiguous'].map(scenario => (
              <button
                key={scenario}
                onClick={() => setSelectedScenario(scenario)}
                className={`px-4 py-2 rounded-lg font-medium transition-all ${
                  selectedScenario === scenario
                    ? 'bg-red-600 text-white'
                    : 'bg-slate-800 text-slate-400 hover:bg-slate-700'
                }`}
              >
                {scenario === 'basic' && 'Simple Case'}
                {scenario === 'comma-in-data' && 'Comma in Data'}
                {scenario === 'ambiguous' && 'Ambiguity Demo'}
              </button>
            ))}
          </div>

          {selectedScenario === 'basic' && (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <div className="bg-slate-950 rounded-xl p-6 border border-slate-700">
                <div className="text-emerald-400 font-mono text-xs uppercase tracking-wider mb-3">Original Array</div>
                <div className="font-mono text-lg text-slate-200">["apple", "banana", "cherry"]</div>
              </div>
              <div className="bg-slate-950 rounded-xl p-6 border border-slate-700">
                <div className="text-cyan-400 font-mono text-xs uppercase tracking-wider mb-3">Comma Separated</div>
                <div className="font-mono text-lg text-slate-200">apple,banana,cherry</div>
                <div className="text-emerald-400 text-sm mt-2">✓ Works perfectly</div>
              </div>
              <div className="bg-slate-950 rounded-xl p-6 border border-slate-700">
                <div className="text-amber-400 font-mono text-xs uppercase tracking-wider mb-3">Reconstruction</div>
                <div className="font-mono text-lg text-slate-200">Split by "," → 3 values</div>
                <div className="text-emerald-400 text-sm mt-2">✓ Correct!</div>
              </div>
            </div>
          )}

          {selectedScenario === 'comma-in-data' && (
            <div className="space-y-6">
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                <div className="bg-slate-950 rounded-xl p-6 border border-emerald-700">
                  <div className="text-emerald-400 font-mono text-xs uppercase tracking-wider mb-3">Original Array</div>
                  <div className="font-mono text-lg text-slate-200">["fg,fg", "dfgh"]</div>
                  <div className="text-slate-500 text-sm mt-2">Two values total</div>
                </div>
                <div className="bg-slate-950 rounded-xl p-6 border border-red-700 animate-pulse">
                  <div className="text-red-400 font-mono text-xs uppercase tracking-wider mb-3">⚠️ Comma Separated</div>
                  <div className="font-mono text-lg text-slate-200">fg,fg,dfgh</div>
                  <div className="text-red-400 text-sm mt-2">Data is AMBIGUOUS!</div>
                </div>
                <div className="bg-slate-950 rounded-xl p-6 border border-red-700">
                  <div className="text-red-400 font-mono text-xs uppercase tracking-wider mb-3">❌ Reconstruction</div>
                  <div className="font-mono text-lg text-slate-200">Split by "," → 3 values?!</div>
                  <div className="text-red-400 text-sm mt-2">WRONG COUNT!</div>
                </div>
              </div>

              <div className="bg-red-950/30 rounded-xl p-6 border border-red-700/50">
                <h4 className="text-red-400 font-bold mb-3">🚨 The Fundamental Problem</h4>
                <p className="text-slate-300 mb-4">
                  When you remove brackets and use comma separation, there's <strong className="text-red-400">NO WAY</strong> to 
                  differentiate between:
                </p>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 font-mono text-sm">
                  <div className="bg-slate-950 p-4 rounded-lg">
                    <div className="text-slate-400 mb-2">Interpretation A:</div>
                    <div className="text-slate-200">["fg,fg", "dfgh"]</div>
                    <div className="text-slate-500 mt-1">→ 2 values</div>
                  </div>
                  <div className="bg-slate-950 p-4 rounded-lg">
                    <div className="text-slate-400 mb-2">Interpretation B:</div>
                    <div className="text-slate-200">["fg", "fg", "dfgh"]</div>
                    <div className="text-slate-500 mt-1">→ 3 values</div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {selectedScenario === 'ambiguous' && (
            <div className="space-y-6">
              <div className="bg-slate-950 rounded-xl p-6 border border-slate-700">
                <h4 className="text-xl font-bold text-amber-400 mb-4">Given this flattened output:</h4>
                <div className="font-mono text-2xl text-center py-4 bg-slate-900 rounded-lg text-amber-300">
                  names: "Smith, John,Doe, Jane"
                </div>
              </div>

              <div className="text-center text-slate-400 text-lg">Which original array produced this?</div>

              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                {[
                  { arr: '["Smith, John", "Doe, Jane"]', count: 2 },
                  { arr: '["Smith", "John", "Doe", "Jane"]', count: 4 },
                  { arr: '["Smith, John,Doe, Jane"]', count: 1 },
                  { arr: '["Smith, John,Doe", "Jane"]', count: 2 }
                ].map((opt, idx) => (
                  <div key={idx} className="bg-slate-950 rounded-xl p-4 border border-slate-700 hover:border-amber-500 transition-colors cursor-pointer">
                    <div className="font-mono text-sm text-slate-300">{opt.arr}</div>
                    <div className="text-amber-400 text-sm mt-2">{opt.count} element(s)</div>
                  </div>
                ))}
              </div>

              <div className="bg-amber-950/30 rounded-xl p-6 border border-amber-700/50 text-center">
                <div className="text-4xl mb-3">🤷</div>
                <p className="text-amber-400 font-bold text-lg">ALL of these are valid interpretations!</p>
                <p className="text-slate-400 mt-2">The data has become <strong>unrecoverable</strong>.</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );

  // Section 2: Nested Array Structure
  const NestedArrayStructure = () => (
    <div className="space-y-8">
      <div className="bg-slate-900 rounded-2xl p-8 border border-slate-700">
        <h3 className="text-2xl font-bold text-cyan-400 mb-6 flex items-center gap-3">
          <span className="text-3xl">📦</span>
          Understanding Nested Array Structure
        </h3>

        {/* Visual Tree */}
        <div className="bg-slate-950 rounded-xl p-8 border border-slate-700 mb-8">
          <div className="text-slate-400 font-mono text-sm mb-6 uppercase tracking-wider">Data Hierarchy Visualization</div>
          
          <div className="flex flex-col items-center">
            {/* Root */}
            <div className="bg-gradient-to-r from-purple-600 to-purple-700 px-6 py-3 rounded-xl font-bold text-white shadow-lg shadow-purple-900/50">
              Order
            </div>
            <div className="w-0.5 h-8 bg-slate-600"></div>
            
            {/* Products Array */}
            <div className="bg-gradient-to-r from-emerald-600 to-emerald-700 px-6 py-2 rounded-xl font-medium text-white">
              products[ ]
            </div>
            <div className="flex gap-24 mt-2">
              <div className="w-0.5 h-8 bg-slate-600"></div>
              <div className="w-0.5 h-8 bg-slate-600"></div>
            </div>
            
            {/* Products */}
            <div className="flex gap-8">
              <div className="flex flex-col items-center">
                <div className="bg-gradient-to-r from-amber-500 to-amber-600 px-4 py-2 rounded-lg font-medium text-white text-sm">
                  Product[0]
                </div>
                <div className="w-0.5 h-6 bg-slate-600"></div>
                <div className="bg-amber-800/50 px-3 py-1 rounded text-amber-300 text-sm">
                  attributes[ ]
                </div>
                <div className="flex gap-2 mt-2">
                  <div className="px-3 py-1 bg-amber-950/50 rounded text-amber-200 text-xs border border-amber-700">RAM</div>
                  <div className="px-3 py-1 bg-amber-950/50 rounded text-amber-200 text-xs border border-amber-700">Storage</div>
                  <div className="px-3 py-1 bg-amber-950/50 rounded text-amber-200 text-xs border border-amber-700">CPU</div>
                </div>
              </div>
              
              <div className="flex flex-col items-center">
                <div className="bg-gradient-to-r from-cyan-500 to-cyan-600 px-4 py-2 rounded-lg font-medium text-white text-sm">
                  Product[1]
                </div>
                <div className="w-0.5 h-6 bg-slate-600"></div>
                <div className="bg-cyan-800/50 px-3 py-1 rounded text-cyan-300 text-sm">
                  attributes[ ]
                </div>
                <div className="flex gap-2 mt-2">
                  <div className="px-3 py-1 bg-cyan-950/50 rounded text-cyan-200 text-xs border border-cyan-700">Bluetooth</div>
                  <div className="px-3 py-1 bg-cyan-950/50 rounded text-cyan-200 text-xs border border-cyan-700">Battery</div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Flattened representation */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-slate-950 rounded-xl p-6 border border-emerald-700">
            <div className="text-emerald-400 font-mono text-sm uppercase tracking-wider mb-4">✓ Correct Flattened Format</div>
            <div className="font-mono bg-slate-900 rounded-lg p-4">
              <span className="text-slate-400">products_attributes_name:</span>
              <div className="mt-2 text-lg">
                <span className="text-amber-400">[</span>
                <span className="text-amber-200">[RAM, Storage, CPU]</span>
                <span className="text-slate-500">, </span>
                <span className="text-cyan-200">[Bluetooth, Battery]</span>
                <span className="text-amber-400">]</span>
              </div>
            </div>
            <div className="mt-4 text-slate-400 text-sm">
              <span className="text-amber-400">Outer array</span> = products (2 items)<br/>
              <span className="text-cyan-400">Inner arrays</span> = attributes per product
            </div>
          </div>

          <div className="bg-slate-950 rounded-xl p-6 border border-red-700">
            <div className="text-red-400 font-mono text-sm uppercase tracking-wider mb-4">✗ If We Remove Outer Brackets</div>
            <div className="font-mono bg-slate-900 rounded-lg p-4">
              <span className="text-slate-400">products_attributes_name:</span>
              <div className="mt-2 text-lg text-red-300">
                [RAM, Storage, CPU, Bluetooth, Battery]
              </div>
            </div>
            <div className="mt-4 text-red-400 text-sm">
              ❌ Lost: Which attrs belong to which product<br/>
              ❌ Lost: Product[0] had 3 attrs, Product[1] had 2
            </div>
          </div>
        </div>
      </div>
    </div>
  );

  // Section 3: Business Simplification Concerns
  const BusinessSimplification = () => (
    <div className="space-y-8">
      <div className="bg-slate-900 rounded-2xl p-8 border border-slate-700">
        <h3 className="text-2xl font-bold text-orange-400 mb-6 flex items-center gap-3">
          <span className="text-3xl">⚠️</span>
          Business Request: "Remove the Bottom Brackets"
        </h3>

        <div className="bg-orange-950/20 rounded-xl p-6 border border-orange-700/50 mb-8">
          <div className="flex items-start gap-4">
            <div className="text-4xl">💼</div>
            <div>
              <h4 className="text-xl font-bold text-orange-400 mb-2">The Business Perspective</h4>
              <p className="text-slate-300 leading-relaxed">
                "If the data is <code className="bg-slate-800 px-2 py-0.5 rounded text-orange-300">[[my number], [his number, his other number]]</code>,
                why can't we simplify it to <code className="bg-slate-800 px-2 py-0.5 rounded text-orange-300">[my number], [his number, his other number]</code>?
                It looks like the same thing!"
              </p>
            </div>
          </div>
        </div>

        {/* Comparison */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <div className="bg-slate-950 rounded-xl p-6 border border-amber-700">
            <h4 className="text-amber-400 font-bold mb-4 text-lg">Format A: With Outer Brackets</h4>
            <div className="font-mono text-xl bg-slate-900 rounded-lg p-4 text-center">
              <span className="text-emerald-400">[</span>
              <span className="text-amber-300">[my number]</span>
              <span className="text-slate-500">, </span>
              <span className="text-cyan-300">[his number, his other number]</span>
              <span className="text-emerald-400">]</span>
            </div>
            <div className="mt-6 space-y-2 text-sm">
              <div className="flex items-center gap-2">
                <span className="text-emerald-400">✓</span>
                <span className="text-slate-300">Clearly an array of 2 arrays</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-emerald-400">✓</span>
                <span className="text-slate-300">First array has 1 item</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-emerald-400">✓</span>
                <span className="text-slate-300">Second array has 2 items</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-emerald-400">✓</span>
                <span className="text-slate-300">Can be perfectly reconstructed</span>
              </div>
            </div>
          </div>

          <div className="bg-slate-950 rounded-xl p-6 border border-red-700">
            <h4 className="text-red-400 font-bold mb-4 text-lg">Format B: Without Outer Brackets</h4>
            <div className="font-mono text-xl bg-slate-900 rounded-lg p-4 text-center">
              <span className="text-amber-300">[my number]</span>
              <span className="text-slate-500">, </span>
              <span className="text-cyan-300">[his number, his other number]</span>
            </div>
            <div className="mt-6 space-y-2 text-sm">
              <div className="flex items-center gap-2">
                <span className="text-red-400">✗</span>
                <span className="text-slate-300">Is this 2 separate arrays? Or a list?</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-red-400">✗</span>
                <span className="text-slate-300">Could be values in a CSV row</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-red-400">✗</span>
                <span className="text-slate-300">No way to know count at parse time</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-red-400">✗</span>
                <span className="text-slate-300">Parsing becomes context-dependent</span>
              </div>
            </div>
          </div>
        </div>

        {/* Critical Issue */}
        <div className="mt-8 bg-gradient-to-r from-red-900/30 to-pink-900/30 rounded-xl p-6 border border-red-700/50">
          <h4 className="text-red-400 font-bold text-xl mb-4 flex items-center gap-2">
            <span>🚨</span> The Critical Parsing Issue
          </h4>
          <div className="text-slate-300 space-y-4">
            <p>
              Without the outer brackets, the string <code className="bg-slate-800 px-2 py-0.5 rounded text-red-300">[my number], [his number, his other number]</code> becomes 
              <strong className="text-red-400"> indistinguishable from a different structure:</strong>
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 font-mono text-sm">
              <div className="bg-slate-950 p-4 rounded-lg border border-slate-700">
                <div className="text-slate-400 mb-2">Could mean:</div>
                <div className="text-slate-200">2 arrays as children of same parent</div>
              </div>
              <div className="bg-slate-950 p-4 rounded-lg border border-slate-700">
                <div className="text-slate-400 mb-2">Could also mean:</div>
                <div className="text-slate-200">A CSV field containing "[a], [b]"</div>
              </div>
            </div>
            <p className="text-amber-400 font-medium">
              The outer brackets are the ONLY signal that tells the parser: "treat these as a collection of arrays"
            </p>
          </div>
        </div>
      </div>
    </div>
  );

  // Section 4: Solutions
  const Solutions = () => (
    <div className="space-y-8">
      <div className="bg-slate-900 rounded-2xl p-8 border border-slate-700">
        <h3 className="text-2xl font-bold text-emerald-400 mb-6 flex items-center gap-3">
          <span className="text-3xl">✅</span>
          Solutions & Trade-offs
        </h3>

        <div className="grid grid-cols-1 gap-6">
          {/* Solution 1: Keep Brackets (Recommended) */}
          <div className="bg-gradient-to-r from-emerald-900/30 to-green-900/30 rounded-xl p-6 border border-emerald-700/50">
            <div className="flex items-start justify-between mb-4">
              <h4 className="text-emerald-400 font-bold text-xl">Solution 1: Keep Bracket Structure (RECOMMENDED)</h4>
              <span className="px-3 py-1 bg-emerald-700 text-emerald-100 rounded-full text-sm font-medium">Best Option</span>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <div className="font-mono bg-slate-950 rounded-lg p-4 mb-4">
                  <div className="text-slate-400 text-sm mb-2">Format:</div>
                  <div className="text-emerald-300">[[value1, value2], [value3]]</div>
                </div>
                <div className="space-y-2 text-sm">
                  <div className="flex items-center gap-2 text-emerald-400">✓ Preserves all structural information</div>
                  <div className="flex items-center gap-2 text-emerald-400">✓ Handles commas in data</div>
                  <div className="flex items-center gap-2 text-emerald-400">✓ Always reconstructable</div>
                  <div className="flex items-center gap-2 text-emerald-400">✓ No data loss possible</div>
                </div>
              </div>
              <div>
                <div className="text-amber-400 text-sm mb-2">Trade-offs:</div>
                <div className="space-y-2 text-sm text-slate-400">
                  <div>• Slightly more verbose</div>
                  <div>• Business sees "extra brackets"</div>
                  <div>• May need explanation to stakeholders</div>
                </div>
              </div>
            </div>
          </div>

          {/* Solution 2: Alternative Delimiters */}
          <div className="bg-slate-950 rounded-xl p-6 border border-amber-700/50">
            <div className="flex items-start justify-between mb-4">
              <h4 className="text-amber-400 font-bold text-xl">Solution 2: Alternative Delimiters</h4>
              <span className="px-3 py-1 bg-amber-700 text-amber-100 rounded-full text-sm font-medium">Partial Fix</span>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
              <div className="bg-slate-900 rounded-lg p-4">
                <div className="text-slate-400 text-sm mb-2">Pipe Separated:</div>
                <div className="font-mono text-amber-300">value1|value2|value3</div>
              </div>
              <div className="bg-slate-900 rounded-lg p-4">
                <div className="text-slate-400 text-sm mb-2">Tab Separated:</div>
                <div className="font-mono text-amber-300">value1\tvalue2\tvalue3</div>
              </div>
              <div className="bg-slate-900 rounded-lg p-4">
                <div className="text-slate-400 text-sm mb-2">Custom (§):</div>
                <div className="font-mono text-amber-300">value1§value2§value3</div>
              </div>
            </div>
            <div className="bg-red-950/30 rounded-lg p-4 border border-red-700/30">
              <div className="text-red-400 font-medium mb-2">⚠️ Warning: Only shifts the problem</div>
              <p className="text-slate-400 text-sm">
                Any delimiter you choose could appear in the data. Pipe-separated fails if data contains "|".
                This is a game of whack-a-mole, not a solution.
              </p>
            </div>
          </div>

          {/* Solution 3: JSON Format */}
          <div className="bg-slate-950 rounded-xl p-6 border border-cyan-700/50">
            <div className="flex items-start justify-between mb-4">
              <h4 className="text-cyan-400 font-bold text-xl">Solution 3: JSON Array Format</h4>
              <span className="px-3 py-1 bg-cyan-700 text-cyan-100 rounded-full text-sm font-medium">Enterprise Standard</span>
            </div>
            <div className="font-mono bg-slate-900 rounded-lg p-4 mb-4">
              <div className="text-slate-400 text-sm mb-2">Format:</div>
              <div className="text-cyan-300">[["fg,fg", "dfgh"], ["another"]]</div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2 text-sm">
                <div className="flex items-center gap-2 text-emerald-400">✓ Handles ALL special characters</div>
                <div className="flex items-center gap-2 text-emerald-400">✓ Industry standard format</div>
                <div className="flex items-center gap-2 text-emerald-400">✓ Native parser support everywhere</div>
              </div>
              <div className="space-y-2 text-sm text-slate-400">
                <div>• Adds quotes around strings</div>
                <div>• Business doesn't like "extra quotes"</div>
                <div>• But: this IS the proper solution</div>
              </div>
            </div>
          </div>

          {/* Solution 4: Escape Sequences */}
          <div className="bg-slate-950 rounded-xl p-6 border border-purple-700/50">
            <div className="flex items-start justify-between mb-4">
              <h4 className="text-purple-400 font-bold text-xl">Solution 4: Escape Sequences</h4>
              <span className="px-3 py-1 bg-purple-700 text-purple-100 rounded-full text-sm font-medium">Complex</span>
            </div>
            <div className="font-mono bg-slate-900 rounded-lg p-4 mb-4">
              <div className="text-slate-400 text-sm mb-2">Example:</div>
              <div className="text-purple-300">fg\,fg,dfgh → ["fg,fg", "dfgh"]</div>
            </div>
            <div className="bg-amber-950/30 rounded-lg p-4 border border-amber-700/30">
              <div className="text-amber-400 font-medium mb-2">⚠️ Complications:</div>
              <ul className="text-slate-400 text-sm list-disc list-inside space-y-1">
                <li>Need to escape the escape character too (\\)</li>
                <li>Makes data harder to read</li>
                <li>Requires custom parsing logic</li>
                <li>Business sees "weird backslashes"</li>
              </ul>
            </div>
          </div>
        </div>

        {/* Final Recommendation */}
        <div className="mt-8 bg-gradient-to-r from-emerald-900/40 to-cyan-900/40 rounded-xl p-8 border border-emerald-700/50">
          <h4 className="text-2xl font-bold text-white mb-4 flex items-center gap-3">
            <span>🎯</span> Final Recommendation
          </h4>
          <div className="text-slate-200 text-lg leading-relaxed space-y-4">
            <p>
              <strong className="text-emerald-400">Keep the current BRACKET_LIST format with full nesting:</strong> 
              <code className="bg-slate-800 px-2 py-1 rounded ml-2 text-amber-300">[[a, b], [c, d, e]]</code>
            </p>
            <p>
              The "extra" brackets are not redundant — they encode essential structural information that 
              allows perfect reconstruction. Removing them would make the data <span className="text-red-400">unrecoverable</span>.
            </p>
            <p className="text-amber-400 font-medium">
              Educating business stakeholders on why this structure is necessary is easier than 
              debugging data corruption issues that arise from "simplified" formats.
            </p>
          </div>
        </div>
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-slate-950 text-white">
      {/* Header */}
      <div className="bg-gradient-to-r from-slate-900 via-slate-800 to-slate-900 border-b border-slate-700">
        <div className="max-w-7xl mx-auto px-6 py-8">
          <div className="flex items-center gap-4 mb-4">
            <div className="w-12 h-12 bg-gradient-to-br from-amber-500 to-orange-600 rounded-xl flex items-center justify-center text-2xl shadow-lg shadow-amber-900/50">
              📊
            </div>
            <div>
              <h1 className="text-3xl font-bold bg-gradient-to-r from-amber-400 to-orange-400 bg-clip-text text-transparent">
                MapFlattener Array Serialization
              </h1>
              <p className="text-slate-400">Understanding why bracket preservation is critical for data integrity</p>
            </div>
          </div>
          
          {/* Navigation */}
          <div className="flex gap-2 mt-6 flex-wrap">
            {sections.map((section) => (
              <button
                key={section.id}
                onClick={() => setActiveSection(section.id)}
                className={`px-4 py-2 rounded-lg font-medium transition-all flex items-center gap-2 ${
                  activeSection === section.id
                    ? 'bg-amber-600 text-white shadow-lg shadow-amber-900/50'
                    : 'bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200'
                }`}
              >
                <span>{section.icon}</span>
                <span className="hidden sm:inline">{section.title}</span>
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-7xl mx-auto px-6 py-8">
        {activeSection === 0 && <WhyBracketsMatter />}
        {activeSection === 1 && <DelimiterCollision />}
        {activeSection === 2 && <NestedArrayStructure />}
        {activeSection === 3 && <BusinessSimplification />}
        {activeSection === 4 && <Solutions />}
      </div>

      {/* Footer with Code Reference */}
      <div className="bg-slate-900 border-t border-slate-700 mt-8">
        <div className="max-w-7xl mx-auto px-6 py-6">
          <div className="flex flex-wrap justify-between items-center gap-4">
            <div className="text-slate-400 text-sm">
              <span className="text-amber-400 font-mono">MapFlattener</span> & <span className="text-cyan-400 font-mono">AvroReconstructor</span> — io.github.pierce
            </div>
            <div className="flex gap-4 text-sm">
              <span className="text-slate-500">Array Formats: JSON | COMMA_SEPARATED | PIPE_SEPARATED | BRACKET_LIST</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default FlatteningVisualization;
