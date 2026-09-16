#!/usr/bin/env node

/**
 * OPTIO Cross-Platform Environment Diagnostic Probe
 * Evaluates host capabilities (OS, Node.js, Git, Make, Docker CLI & Daemon).
 * Fully self-contained (zero external npm dependencies).
 */

const os = require('os');
const { execSync } = require('child_process');

function runProbe(cmd) {
  try {
    const output = execSync(cmd, {
      stdio: ['pipe', 'pipe', 'pipe'],
      encoding: 'utf8',
      timeout: 6000
    });
    return { ok: true, output: output.trim() };
  } catch (err) {
    const msg = (err.stdout || err.stderr || err.message || '').toString().trim();
    return { ok: false, output: msg };
  }
}

console.log('======================================================================');
console.log('             OPTIO SYSTEM ENVIRONMENT DIAGNOSTIC PROBE               ');
console.log('======================================================================');

// 1. Host Architecture & Node.js
console.log('\n[1] Host & Runtime Environment:');
console.log(`  - OS Platform:    ${os.platform()} (${os.type()} ${os.release()})`);
console.log(`  - Architecture:   ${os.arch()}`);
console.log(`  - CPU Cores:      ${os.cpus().length}`);
console.log(`  - Total Memory:   ${(os.totalmem() / 1024 / 1024 / 1024).toFixed(2)} GB`);
console.log(`  - Free Memory:    ${(os.freemem() / 1024 / 1024 / 1024).toFixed(2)} GB`);
console.log(`  - Node.js:        ${process.version} (V8 ${process.versions.v8})`);

// 2. Developer Tooling (Git & Make)
console.log('\n[2] Developer Tooling:');
const gitProbe = runProbe('git --version');
console.log(`  - Git CLI:        ${gitProbe.ok ? gitProbe.output : 'NOT FOUND (' + gitProbe.output + ')'}`);

const makeProbe = runProbe('make --version');
const makeFirstLine = makeProbe.ok ? makeProbe.output.split('\n')[0] : 'NOT FOUND';
console.log(`  - Make:           ${makeFirstLine}`);

// 3. Container Runtime (Docker & Compose)
console.log('\n[3] Container Infrastructure:');
const dockerCliProbe = runProbe('docker --version');
console.log(`  - Docker CLI:     ${dockerCliProbe.ok ? dockerCliProbe.output : 'NOT FOUND'}`);

const composeProbe = runProbe('docker compose version');
console.log(`  - Docker Compose: ${composeProbe.ok ? composeProbe.output : 'NOT FOUND'}`);

const daemonProbe = runProbe('docker info --format "{{.ServerVersion}}"');
let daemonRunning = false;

if (daemonProbe.ok && daemonProbe.output && !daemonProbe.output.toLowerCase().includes('error')) {
  daemonRunning = true;
  console.log(`  - Docker Daemon:   RUNNING (Server v${daemonProbe.output})`);
} else {
  console.log('  - Docker Daemon:   OFFLINE / NOT REACHABLE');
}

// 4. Execution Guidance
console.log('\n----------------------------------------------------------------------');
if (daemonRunning) {
  console.log('STATUS: Environment is fully equipped to execute "docker compose up" and "make verify".');
} else {
  console.log('NOTICE: Docker daemon is currently not running on this host.');
  console.log('  -> Local development, TypeScript typechecking, and unit tests run seamlessly.');
  console.log('  -> For full container integration and "make verify", you can:');
  console.log('     1. Launch Docker Desktop on this machine, OR');
  console.log('     2. Launch a 1-click GitHub Codespaces environment (.devcontainer/devcontainer.json)');
  console.log('        which provides native Linux Docker-in-Docker with zero host setup.');
}
console.log('======================================================================\n');

// Resilient exit code 0
process.exit(0);
