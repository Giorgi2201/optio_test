/**
 * Gate 5 Verification Logic & Unified Verification Orchestrator Test Suite
 * Validates observability and introspection assertions, answers to the 5 operational questions,
 * and verifies that the orchestrator aggregates all 5 gates into the standardized summary report.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { formatGateResult, evaluateGate5Observability } = require('../common.js');
const { runGate5 } = require('../gate5.js');
const { runVerification, parseArgs } = require('../index.js');

const validTelemetryFixture = {
  backfill_cursor: 500000,
  backfill_completion_pct: 100.0,
  current_throughput_eps: 12500,
  incremental_lag_records: 0,
  incremental_lag_ms: 12,
  dlq_pending_count: 3,
  health: {
    overall: 'HEALTHY',
    postgres: { status: 'UP', latencyMs: 1.2 },
    elasticsearch: { status: 'UP', latencyMs: 4.5 },
    rabbitmq: { status: 'UP', latencyMs: 2.1 }
  }
};

describe('Gate 5 Verification - Observability & Introspection', () => {
  it('1. Target Formatting: Validates formatted output matching exact specification', () => {
    const result = evaluateGate5Observability(validTelemetryFixture);

    assert.equal(result.passed, true);
    assert.equal(result.output, 'G5 observability ................ PASS');
  });

  it('2. Valid Telemetry: Asserts all 5 operational questions are answered cleanly', () => {
    const result = evaluateGate5Observability(validTelemetryFixture);

    assert.equal(result.passed, true);
    assert.ok(result.answers, 'answers object must be present on pass');
    // Q1: Where is the backfill?
    assert.equal(result.answers.backfillPosition, 'cursor: 500000 (100%)');
    // Q2: What is the current throughput?
    assert.equal(result.answers.throughputEps, '12500 eps');
    // Q3: How much incremental lag is there?
    assert.equal(result.answers.incrementalLag, '0 records (12ms)');
    // Q4: How many records are in the DLQ?
    assert.equal(result.answers.dlqPending, '3 pending');
    // Q5: Is the system healthy or not?
    assert.equal(result.answers.systemHealth, 'HEALTHY');
  });

  it('3. Invariant Rejection: Rejects null, undefined, or non-object telemetry payload', () => {
    const resNull = evaluateGate5Observability(null);
    assert.equal(resNull.passed, false);
    assert.match(resNull.output, /FAIL/);
    assert.match(resNull.details, /no telemetry payload received/);

    const resUndefined = evaluateGate5Observability(undefined);
    assert.equal(resUndefined.passed, false);
    assert.match(resUndefined.output, /FAIL/);
  });

  it('4. Invariant Rejection: Rejects missing or non-number backfill position metrics', () => {
    const badCursor = { ...validTelemetryFixture, backfill_cursor: 'invalid' };
    const resCursor = evaluateGate5Observability(badCursor);
    assert.equal(resCursor.passed, false);
    assert.match(resCursor.details, /backfill position/);

    const missingPct = { ...validTelemetryFixture, backfill_completion_pct: undefined };
    const resPct = evaluateGate5Observability(missingPct);
    assert.equal(resPct.passed, false);
    assert.match(resPct.details, /backfill position/);
  });

  it('5. Invariant Rejection: Rejects negative or missing throughput_eps', () => {
    const negativeEps = { ...validTelemetryFixture, current_throughput_eps: -100 };
    const resNegative = evaluateGate5Observability(negativeEps);
    assert.equal(resNegative.passed, false);
    assert.match(resNegative.details, /throughput_eps/);

    const missingEps = { ...validTelemetryFixture, current_throughput_eps: null };
    const resMissing = evaluateGate5Observability(missingEps);
    assert.equal(resMissing.passed, false);
    assert.match(resMissing.details, /throughput_eps/);
  });

  it('6. Invariant Rejection: Rejects missing or negative incremental lag metrics', () => {
    const negativeLagRecords = { ...validTelemetryFixture, incremental_lag_records: -5 };
    const resRecords = evaluateGate5Observability(negativeLagRecords);
    assert.equal(resRecords.passed, false);
    assert.match(resRecords.details, /incremental lag metrics/);

    const negativeLagMs = { ...validTelemetryFixture, incremental_lag_ms: -10 };
    const resMs = evaluateGate5Observability(negativeLagMs);
    assert.equal(resMs.passed, false);
    assert.match(resMs.details, /incremental lag metrics/);
  });

  it('7. Invariant Rejection: Rejects negative or non-number DLQ pending count', () => {
    const negativeDLQ = { ...validTelemetryFixture, dlq_pending_count: -1 };
    const resDLQ = evaluateGate5Observability(negativeDLQ);
    assert.equal(resDLQ.passed, false);
    assert.match(resDLQ.details, /dlq_pending_count/);
  });

  it('8. Invariant Rejection: Rejects missing or invalid health status structure', () => {
    const invalidOverall = {
      ...validTelemetryFixture,
      health: { ...validTelemetryFixture.health, overall: 'CRITICAL_FAILURE' }
    };
    const resOverall = evaluateGate5Observability(invalidOverall);
    assert.equal(resOverall.passed, false);
    assert.match(resOverall.details, /health status structure/);

    const missingSinkHealth = {
      ...validTelemetryFixture,
      health: { overall: 'HEALTHY', postgres: { status: 'UP' } } // missing elasticsearch & rabbitmq
    };
    const resMissingSink = evaluateGate5Observability(missingSinkHealth);
    assert.equal(resMissingSink.passed, false);
    assert.match(resMissingSink.details, /health status structure/);
  });

  it('9. Gate 5 Runner: Evaluates directly injected telemetry without network polling', async () => {
    const result = await runGate5({
      telemetry: validTelemetryFixture,
      silent: true
    });

    assert.equal(result.passed, true);
    assert.equal(result.output, 'G5 observability ................ PASS');
  });

  it('10. Gate 5 Runner: Polls telemetry endpoint and handles failure gracefully when unreachable', async () => {
    const mockGetTelemetry = async () => null;
    const mockSleep = async () => {};

    const result = await runGate5({
      getTelemetry: mockGetTelemetry,
      sleep: mockSleep,
      maxWaitMs: 50,
      pollIntervalMs: 10,
      silent: true
    });

    assert.equal(result.passed, false);
    assert.match(result.output, /FAIL/);
  });
});

describe('Unified Verification Orchestrator - scripts/verify/index.js', () => {
  it('11. CLI Argument Parsing: Correctly parses --gate, --quick, --bail, and rejects invalid gates', () => {
    const parsed1 = parseArgs(['--gate', '3', '--quick', '--bail']);
    assert.equal(parsed1.gate, 3);
    assert.equal(parsed1.quick, true);
    assert.equal(parsed1.bail, true);

    const parsed2 = parseArgs(['-g', '5', '-q']);
    assert.equal(parsed2.gate, 5);
    assert.equal(parsed2.quick, true);

    const parsedPositional = parseArgs(['4']);
    assert.equal(parsedPositional.gate, 4);

    assert.throws(() => parseArgs(['--gate', '9']), /Invalid --gate argument/);
    assert.throws(() => parseArgs(['--gate', '0']), /Invalid --gate argument/);
  });

  it('12. Orchestrator Summary: Asserts that orchestrator properly aggregates results from all 5 gates and produces the standardized 5-line report block', async () => {
    const mockRunners = {
      1: async () => ({
        passed: true,
        output: 'G1 resume after kill ............ PASS (killed at 412,331 / resumed at 412,000, 0 lost)'
      }),
      2: async () => ({
        passed: true,
        output: 'G2 no duplicates ................ PASS (2,000,000 source / 2,000,000 sink / 0 dupes)'
      }),
      3: async () => ({
        passed: true,
        output: 'G3 sink outage .................. PASS (60s down, 0 lost, recovered in 4.2s)'
      }),
      4: async () => ({
        passed: true,
        output: 'G4 partial batch failure ........ PASS (497 written, 3 in DLQ)'
      }),
      5: async () => ({
        passed: true,
        output: 'G5 observability ................ PASS'
      })
    };

    const result = await runVerification({
      runners: mockRunners,
      silent: true,
      closeDb: false
    });

    assert.equal(result.success, true);
    assert.equal(result.passedCount, 5);
    assert.equal(result.totalCount, 5);
    assert.equal(result.results.length, 5);

    // Assert that the 5-line summary block matches the exact specification
    const reportLines = result.results.map((r) => r.output);
    assert.equal(
      reportLines[0],
      'G1 resume after kill ............ PASS (killed at 412,331 / resumed at 412,000, 0 lost)'
    );
    assert.equal(
      reportLines[1],
      'G2 no duplicates ................ PASS (2,000,000 source / 2,000,000 sink / 0 dupes)'
    );
    assert.equal(
      reportLines[2],
      'G3 sink outage .................. PASS (60s down, 0 lost, recovered in 4.2s)'
    );
    assert.equal(
      reportLines[3],
      'G4 partial batch failure ........ PASS (497 written, 3 in DLQ)'
    );
    assert.equal(
      reportLines[4],
      'G5 observability ................ PASS'
    );

    // Verify formatted banner and summary lines in overall report
    assert.match(result.report, /KILL IT TWICE: RESILIENCE VERIFICATION SUITE/);
    assert.match(result.report, /ALL RESILIENCE GATES PASSED \[5\/5\]/);
  });

  it('13. Orchestrator Single Gate: Executes only selected gate when --gate is specified', async () => {
    let gate1Called = false;
    let gate5Called = false;

    const mockRunners = {
      1: async () => {
        gate1Called = true;
        return { passed: true, output: 'G1 resume after kill ............ PASS' };
      },
      5: async () => {
        gate5Called = true;
        return { passed: true, output: 'G5 observability ................ PASS' };
      }
    };

    const result = await runVerification({
      gate: 5,
      runners: mockRunners,
      silent: true,
      closeDb: false
    });

    assert.equal(result.success, true);
    assert.equal(result.passedCount, 1);
    assert.equal(result.totalCount, 1);
    assert.equal(gate1Called, false);
    assert.equal(gate5Called, true);
    assert.match(result.report, /GATE 5 RESILIENCE GATE PASSED \[1\/1\]/);
  });

  it('14. Orchestrator Failure Handling: Correctly reports failures and sets exit status to false', async () => {
    const mockRunners = {
      1: async () => ({
        passed: true,
        output: 'G1 resume after kill ............ PASS'
      }),
      2: async () => {
        throw new Error('Connection refused to Elasticsearch');
      },
      3: async () => ({
        passed: false,
        output: 'G3 sink outage .................. FAIL (timeout waiting for recovery)'
      }),
      4: async () => ({
        passed: true,
        output: 'G4 partial batch failure ........ PASS (497 written, 3 in DLQ)'
      }),
      5: async () => ({
        passed: true,
        output: 'G5 observability ................ PASS'
      })
    };

    const result = await runVerification({
      runners: mockRunners,
      silent: true,
      closeDb: false
    });

    assert.equal(result.success, false);
    assert.equal(result.passedCount, 3);
    assert.equal(result.totalCount, 5);
    assert.match(result.report, /VERIFICATION FAILED \[3\/5 passed\]/);
    assert.match(result.results[1].output, /FAIL \(Connection refused to Elasticsearch\)/);
  });
});
