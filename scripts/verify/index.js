#!/usr/bin/env node

/**
 * Unified Verification Orchestrator for 'make verify' and './verify.sh'.
 * Sequentially executes all five Kill-It-Twice resilience verification gates:
 * - Gate 1: Crash Recovery & Watermark Resumption
 * - Gate 2: Deduplication & Effectively-Once Delivery
 * - Gate 3: Receiver Outage, Anti-Busy-Loop & Self-Healing
 * - Gate 4: Partial Batch Failure & DLQ Isolation
 * - Gate 5: Observability & Introspection Verification
 *
 * Emits the standardized 5-gate resilience verification report.
 */

const { closeDatabase, formatGateResult } = require('./common.js');
const { runGate1 } = require('./gate1.js');
const { runGate2 } = require('./gate2.js');
const { runGate3 } = require('./gate3.js');
const { runGate4 } = require('./gate4.js');
const { runGate5 } = require('./gate5.js');

const defaultRunners = {
  1: runGate1,
  2: runGate2,
  3: runGate3,
  4: runGate4,
  5: runGate5
};

const gateNames = {
  1: 'G1 resume after kill',
  2: 'G2 no duplicates',
  3: 'G3 sink outage',
  4: 'G4 partial batch failure',
  5: 'G5 observability'
};

/**
 * Parses command line arguments for the verification harness.
 */
function parseArgs(args = process.argv.slice(2)) {
  const options = {
    gate: null,
    quick: false,
    bail: false,
    help: false
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--gate' || arg === '-g') {
      const val = parseInt(args[++i], 10);
      if (val >= 1 && val <= 5) {
        options.gate = val;
      } else {
        throw new Error(`Invalid --gate argument "${args[i]}". Must be an integer 1 through 5.`);
      }
    } else if (arg.startsWith('--gate=')) {
      const val = parseInt(arg.split('=')[1], 10);
      if (val >= 1 && val <= 5) {
        options.gate = val;
      } else {
        throw new Error(`Invalid --gate argument. Must be an integer 1 through 5.`);
      }
    } else if (/^[1-5]$/.test(arg)) {
      options.gate = parseInt(arg, 10);
    } else if (arg === '--quick' || arg === '-q') {
      options.quick = true;
    } else if (arg === '--bail') {
      options.bail = true;
    } else if (arg === '--help' || arg === '-h') {
      options.help = true;
    }
  }

  return options;
}

/**
 * Prints CLI usage help.
 */
function printHelp() {
  console.log(`
Usage: node scripts/verify/index.js [options]

Options:
  --gate, -g <1-5>   Run only the specified resilience gate (1 through 5)
  --quick, -q        Run in fast/accelerated mode with reduced timeouts
  --bail             Abort immediately on first gate failure
  --help, -h         Display this help message
`);
}

/**
 * Unified verification runner executing specified gates and printing the summary table.
 */
async function runVerification(options = {}) {
  const runners = { ...defaultRunners, ...options.runners };
  const quick = Boolean(options.quick);
  const bail = Boolean(options.bail);
  const targetGates = options.gate ? [options.gate] : [1, 2, 3, 4, 5];

  const separator = '='.repeat(70);
  const bannerHeader = [
    separator,
    '          KILL IT TWICE: RESILIENCE VERIFICATION SUITE               ',
    separator
  ].join('\n');

  if (!options.silent) {
    console.log(bannerHeader);
  }

  const startTime = Date.now();
  const results = [];
  let allPassed = true;

  try {
    for (const g of targetGates) {
      const gateOpts = {
        closeDb: false,
        silent: options.silent,
        ...options.gateOptions
      };

      if (quick) {
        if (g === 1) gateOpts.maxWaitMs = 15000;
        if (g === 2) gateOpts.maxWaitMs = 10000;
        if (g === 3) {
          gateOpts.outageDurationMs = 5000;
          gateOpts.maxRecoveryWaitMs = 15000;
        }
        if (g === 4) gateOpts.maxWaitMs = 10000;
        if (g === 5) gateOpts.maxWaitMs = 3000;
      }

      const runner = runners[g];
      if (!runner) {
        throw new Error(`No runner registered for Gate ${g}`);
      }

      try {
        const res = await runner(gateOpts);
        results.push({
          gate: g,
          passed: Boolean(res?.passed),
          output: res?.output || formatGateResult(gateNames[g], res?.passed ? 'PASS' : 'FAIL', ''),
          details: res
        });

        if (!res?.passed) {
          allPassed = false;
          if (bail) break;
        }
      } catch (err) {
        allPassed = false;
        const errOutput = formatGateResult(gateNames[g], 'FAIL', err.message || 'runner error');
        if (!options.silent) {
          console.log(errOutput);
        }
        results.push({
          gate: g,
          passed: false,
          output: errOutput,
          error: err
        });
        if (bail) break;
      }
    }
  } finally {
    if (options.closeDb !== false) {
      try {
        await closeDatabase();
      } catch {
        // Ignore DB closure errors during teardown
      }
    }
  }

  const durationSec = ((Date.now() - startTime) / 1000).toFixed(1);
  const passedCount = results.filter((r) => r.passed).length;
  const totalCount = results.length;

  const summaryTitle = allPassed
    ? (totalCount === 1 ? `GATE ${targetGates[0]} RESILIENCE GATE PASSED [1/1]` : `ALL RESILIENCE GATES PASSED [${passedCount}/${totalCount}]`)
    : `VERIFICATION FAILED [${passedCount}/${totalCount} passed]`;

  if (!options.silent) {
    console.log(separator);
    console.log(summaryTitle);
    console.log(separator);
  }

  const report = [
    bannerHeader,
    ...results.map((r) => r.output),
    separator,
    summaryTitle,
    separator
  ].join('\n');

  return {
    success: allPassed,
    passedCount,
    totalCount,
    results,
    report,
    durationSec
  };
}

// Auto-execute if invoked directly as CLI script
if (require.main === module) {
  let parsed;
  try {
    parsed = parseArgs(process.argv.slice(2));
  } catch (err) {
    console.error(`[ARGUMENT ERROR] ${err.message}`);
    process.exit(1);
  }

  if (parsed.help) {
    printHelp();
    process.exit(0);
  }

  runVerification(parsed)
    .then((result) => {
      process.exit(result.success ? 0 : 1);
    })
    .catch((err) => {
      console.error('[FATAL ERROR]', err);
      process.exit(1);
    });
}

module.exports = {
  parseArgs,
  printHelp,
  runVerification,
  defaultRunners
};
