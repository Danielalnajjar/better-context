import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { createAnonymousDirectoryKey, createResourcesService } from '../service.ts';
import { setFilesystemLockTestHookForTests } from '../lock.ts';
import {
	getGitLockPath,
	getGitMirrorPath,
	getGitMirrorRepoPath,
	getTmpCacheRoot
} from '../layout.ts';
import type { ResourceDefinition } from '../schema.ts';
import { createConfigServiceMock, createStaleLockDirectory } from '../test-support.ts';
import { logQueueMetrics, readStream, waitFor } from './test-fixtures/process-support.ts';

const workerPath = path.join(import.meta.dir, 'test-fixtures', 'git-materialize-worker.ts');

const runGit = async (
	args: readonly string[],
	options?: { cwd?: string; env?: Record<string, string | undefined> }
) => {
	const proc = Bun.spawn(['git', ...args], {
		...(options?.cwd ? { cwd: options.cwd } : {}),
		...(options?.env ? { env: { ...process.env, ...options.env } } : {}),
		stdout: 'pipe',
		stderr: 'pipe'
	});

	const [stdout, stderr, exitCode] = await Promise.all([
		readStream(proc.stdout),
		readStream(proc.stderr),
		proc.exited
	]);

	if (exitCode !== 0) {
		throw new Error(
			`git ${args.join(' ')} failed with exit code ${exitCode}\nstdout:\n${stdout}\nstderr:\n${stderr}`
		);
	}

	return stdout.trim();
};

const pathExists = async (targetPath: string) => {
	try {
		await fs.stat(targetPath);
		return true;
	} catch {
		return false;
	}
};

const createRealGitRemote = async (tempDir: string) => {
	const bareRepoPath = path.join(tempDir, 'real-remote.git');
	const seedRepoPath = path.join(tempDir, 'real-seed');
	const homePath = path.join(tempDir, 'real-home');
	const gitConfigPath = path.join(tempDir, 'real-gitconfig');
	const remoteUrl = 'https://example.com/repo.git';
	const redirectedUrl = pathToFileURL(bareRepoPath).href;
	const gitEnv = {
		HOME: homePath,
		GIT_CONFIG_GLOBAL: gitConfigPath,
		GIT_CONFIG_NOSYSTEM: '1'
	};

	await fs.mkdir(homePath, { recursive: true });
	await Bun.write(
		gitConfigPath,
		`[url "${redirectedUrl}"]\n\tinsteadOf = ${remoteUrl}\n[init]\n\tdefaultBranch = main\n`
	);

	await runGit(['init', '--bare', bareRepoPath], { env: gitEnv });
	await runGit(['init', seedRepoPath], { env: gitEnv });
	await runGit(['config', 'user.name', 'BTCA Test'], { cwd: seedRepoPath, env: gitEnv });
	await runGit(['config', 'user.email', 'btca@example.com'], { cwd: seedRepoPath, env: gitEnv });

	await fs.mkdir(path.join(seedRepoPath, 'docs'), { recursive: true });
	await Bun.write(path.join(seedRepoPath, 'README.md'), '# real repo\n');
	await Bun.write(path.join(seedRepoPath, 'docs', 'guide.md'), 'real docs\n');
	await runGit(['add', '.'], { cwd: seedRepoPath, env: gitEnv });
	await runGit(['commit', '-m', 'initial'], { cwd: seedRepoPath, env: gitEnv });
	await runGit(['remote', 'add', 'origin', bareRepoPath], { cwd: seedRepoPath, env: gitEnv });
	await runGit(['push', '--set-upstream', 'origin', 'main'], { cwd: seedRepoPath, env: gitEnv });

	return { remoteUrl, gitEnv };
};

const createFakeGitBinary = async (
	tempDir: string,
	args?: { sleepMs?: number; holdMutationsUntilRelease?: boolean }
) => {
	const binDir = path.join(tempDir, 'bin');
	const logPath = path.join(tempDir, 'fake-git-log.jsonl');
	const mutationRoot = path.join(tempDir, 'fake-git-mutation-locks');
	const holdRoot = path.join(tempDir, 'fake-git-holds');
	await fs.mkdir(binDir, { recursive: true });
	await fs.mkdir(mutationRoot, { recursive: true });
	await fs.mkdir(holdRoot, { recursive: true });

	const scriptPath = path.join(binDir, 'git');
	const script = `#!/usr/bin/env bun
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

const logPath = process.env.BTCA_FAKE_GIT_LOG;
const mutationRoot = process.env.BTCA_FAKE_GIT_MUTATION_ROOT;
const holdRoot = process.env.BTCA_FAKE_GIT_HOLD_ROOT;
const sleepMs = Number(process.env.BTCA_FAKE_GIT_SLEEP_MS ?? '0');
const args = process.argv.slice(2);

const log = async (event) => {
  if (!logPath) return;
  await fs.appendFile(logPath, JSON.stringify(event) + '\\n');
};

const maybeWaitForRelease = async (key) => {
  if (!holdRoot) return;
  const token = encodeURIComponent(key);
  const startedPath = path.join(holdRoot, token + '.started');
  const releasePath = path.join(holdRoot, token + '.release');
  await fs.writeFile(startedPath, '');
  while (!(await fs.stat(releasePath).catch(() => null))) {
    await sleep(10);
  }
};

const acquireMutationLock = async (key) => {
  if (!mutationRoot) return async () => {};
  const mutationLockPath = path.join(mutationRoot, encodeURIComponent(key) + '.lock');
  const handle = await fs.open(mutationLockPath, 'wx').catch(() => null);
  if (!handle) {
    console.error('concurrent git mutation');
    process.exit(1);
  }
  await handle.close();
  return async () => {
    await fs.rm(mutationLockPath, { force: true }).catch(() => undefined);
  };
};

const repoPath = args[args.length - 1];
if (!repoPath) process.exit(1);

if (args[0] === 'clone') {
  const branchIndex = args.indexOf('-b');
  const branch = branchIndex >= 0 ? args[branchIndex + 1] : 'main';
  const release = await acquireMutationLock(repoPath);
  try {
    await log({ cmd: 'clone', branch, repoPath });
    if (sleepMs > 0) await sleep(sleepMs);
    await maybeWaitForRelease(repoPath);
    await fs.mkdir(path.join(repoPath, '.git'), { recursive: true });
    await fs.mkdir(path.join(repoPath, 'docs'), { recursive: true });
    await fs.writeFile(path.join(repoPath, 'README.md'), '# fake repo\\n');
    await fs.writeFile(path.join(repoPath, 'docs', 'guide.md'), 'guide\\n');
    await fs.writeFile(path.join(repoPath, '.fake-head'), 'fake-commit\\n');
    await fs.writeFile(path.join(repoPath, '.fake-branch'), branch + '\\n');
  } finally {
    await release();
  }
  process.exit(0);
}

if (args[0] === 'fetch' || args[0] === 'reset') {
  const cwd = process.cwd();
  const release = await acquireMutationLock(cwd);
  try {
    await log({ cmd: args[0], cwd });
    if (sleepMs > 0) await sleep(sleepMs);
    await maybeWaitForRelease(cwd);
    await fs.mkdir(path.join(cwd, '.git'), { recursive: true });
    await fs.writeFile(path.join(cwd, '.fake-head'), 'fake-commit\\n');
  } finally {
    await release();
  }
  process.exit(0);
}

if (args[0] === 'sparse-checkout' || args[0] === 'checkout') {
  await log({ cmd: args[0], cwd: process.cwd(), args });
  process.exit(0);
}

if (args[0] === 'rev-parse' && args[1] === 'HEAD') {
  const cwd = process.cwd();
  const value = await fs.readFile(path.join(cwd, '.fake-head'), 'utf8').catch(() => 'fake-commit\\n');
  process.stdout.write(value);
  process.exit(0);
}

if (args[0] === 'rev-parse' && args[1] === '--abbrev-ref' && args[2] === 'HEAD') {
  const cwd = process.cwd();
  const value = await fs.readFile(path.join(cwd, '.fake-branch'), 'utf8').catch(() => 'main\\n');
  process.stdout.write(value);
  process.exit(0);
}

await log({ cmd: 'unknown', args, cwd: process.cwd() });
process.exit(0);
`;

	await Bun.write(scriptPath, script);
	await fs.chmod(scriptPath, 0o755);

	return {
		binDir,
		logPath,
		mutationRoot,
		...(args?.holdMutationsUntilRelease ? { holdRoot } : {}),
		sleepMs: args?.sleepMs ?? 0
	};
};

const runWorker = async (args: {
	binDir?: string;
	logPath?: string;
	mutationRoot?: string;
	holdRoot?: string;
	sleepMs?: number;
	env?: Record<string, string | undefined>;
	payload: Record<string, unknown>;
}) => {
	const proc = Bun.spawn([process.execPath, workerPath], {
		cwd: path.join(import.meta.dir, '..', '..', '..', '..', '..'),
		env: {
			...process.env,
			...(args.binDir ? { PATH: `${args.binDir}:${process.env.PATH ?? ''}` } : {}),
			...(args.logPath ? { BTCA_FAKE_GIT_LOG: args.logPath } : {}),
			...(args.mutationRoot ? { BTCA_FAKE_GIT_MUTATION_ROOT: args.mutationRoot } : {}),
			...(args.holdRoot ? { BTCA_FAKE_GIT_HOLD_ROOT: args.holdRoot } : {}),
			...(typeof args.sleepMs === 'number' ? { BTCA_FAKE_GIT_SLEEP_MS: String(args.sleepMs) } : {}),
			...(args.env ?? {}),
			BTCA_WORKER_PAYLOAD: JSON.stringify({
				...(args.binDir ? { gitBinDir: args.binDir } : {}),
				...args.payload
			})
		},
		stdout: 'pipe',
		stderr: 'pipe'
	});

	const [stdout, stderr, exitCode] = await Promise.all([
		readStream(proc.stdout),
		readStream(proc.stderr),
		proc.exited
	]);

	return { stdout, stderr, exitCode };
};

describe('git process locking', () => {
	let tempDir: string;

	beforeEach(async () => {
		tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-git-process-'));
	});

	afterEach(async () => {
		setFilesystemLockTestHookForTests();
		await fs.rm(tempDir, { recursive: true, force: true });
	});

	it('serializes same named git mirror across concurrent subprocesses', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const fakeGit = await createFakeGitBinary(tempDir, { sleepMs: 120 });

		const payload = {
			resourcesDirectory,
			name: 'docs',
			url: 'https://example.com/repo.git',
			branch: 'main',
			repoSubPaths: ['docs']
		};

		const startedAt = performance.now();
		const [first, second] = await Promise.all([
			(async () => {
				const workerStartedAt = performance.now();
				const result = await runWorker({ ...fakeGit, payload });
				return { ...result, elapsedMs: performance.now() - workerStartedAt };
			})(),
			(async () => {
				const workerStartedAt = performance.now();
				const result = await runWorker({ ...fakeGit, payload });
				return { ...result, elapsedMs: performance.now() - workerStartedAt };
			})()
		]);
		logQueueMetrics('git.named.fake.same-resource', {
			totalElapsedMs: performance.now() - startedAt,
			workerElapsedMs: [first.elapsedMs, second.elapsedMs]
		});

		expect(first.exitCode).toBe(0);
		expect(second.exitCode).toBe(0);
		expect(first.stderr).not.toContain('concurrent git mutation');
		expect(second.stderr).not.toContain('concurrent git mutation');
	});

	it('serializes same named git mirror across concurrent subprocesses with a real local bare repo', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const remote = await createRealGitRemote(tempDir);
		const payload = {
			resourcesDirectory,
			name: 'docs',
			url: remote.remoteUrl,
			branch: 'main',
			repoSubPaths: ['docs']
		};

		const startedAt = performance.now();
		const [first, second] = await Promise.all([
			(async () => {
				const workerStartedAt = performance.now();
				const result = await runWorker({ env: remote.gitEnv, payload });
				return { ...result, elapsedMs: performance.now() - workerStartedAt };
			})(),
			(async () => {
				const workerStartedAt = performance.now();
				const result = await runWorker({ env: remote.gitEnv, payload });
				return { ...result, elapsedMs: performance.now() - workerStartedAt };
			})()
		]);
		logQueueMetrics('git.named.real.same-resource', {
			totalElapsedMs: performance.now() - startedAt,
			workerElapsedMs: [first.elapsedMs, second.elapsedMs]
		});

		expect(first.exitCode).toBe(0);
		expect(second.exitCode).toBe(0);
		expect(first.stderr).toBe('');
		expect(second.stderr).toBe('');
		expect(
			await Bun.file(
				path.join(resourcesDirectory, '.git-mirrors', 'docs', 'repo', 'docs', 'guide.md')
			).exists()
		).toBe(true);
	});

	it('uses a unique temp directory for each anonymous git resource', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const fakeGit = await createFakeGitBinary(tempDir);
		const url = 'https://example.com/repo.git';
		const anonymousKey = createAnonymousDirectoryKey(url);

		const payload = {
			resourcesDirectory,
			name: `anonymous:${url}`,
			url,
			branch: 'main',
			ephemeral: true,
			localDirectoryKey: anonymousKey
		};

		const [first, second] = await Promise.all([
			runWorker({ ...fakeGit, payload }),
			runWorker({ ...fakeGit, payload })
		]);

		expect(first.exitCode).toBe(0);
		expect(second.exitCode).toBe(0);

		const cloneTargets = (await Bun.file(fakeGit.logPath).text())
			.trim()
			.split('\n')
			.filter(Boolean)
			.map((line) => JSON.parse(line) as { cmd: string; repoPath?: string })
			.filter((entry) => entry.cmd === 'clone' && typeof entry.repoPath === 'string')
			.map((entry) => entry.repoPath as string);

		expect(new Set(cloneTargets).size).toBeGreaterThanOrEqual(2);
		for (const cloneTarget of cloneTargets) {
			expect(cloneTarget).toContain(
				`${path.join(getTmpCacheRoot(resourcesDirectory), `btca-anon-git-${anonymousKey}-`)}`
			);
		}
	});

	it('clear waits for an in-flight anonymous git materialization before removing temp dirs', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const fakeGit = await createFakeGitBinary(tempDir, { holdMutationsUntilRelease: true });
		const url = 'https://example.com/repo.git';
		const anonymousKey = createAnonymousDirectoryKey(url);
		const payload = {
			resourcesDirectory,
			name: `anonymous:${url}`,
			url,
			branch: 'main',
			ephemeral: true,
			localDirectoryKey: anonymousKey
		};
		let clearBlocked = false;
		setFilesystemLockTestHookForTests((event) => {
			if (
				event.phase === 'waiting-on-live-lock' &&
				event.lockPath === getGitLockPath(resourcesDirectory, anonymousKey) &&
				event.label === `clear.git.${anonymousKey}`
			) {
				clearBlocked = true;
			}
		});

		const workerPromise = runWorker({ ...fakeGit, payload });

		let cloneStartedEntry = '';
		await waitFor(async () => {
			const entries = await fs.readdir(fakeGit.holdRoot!).catch(() => []);
			cloneStartedEntry = entries.find((entry) => entry.endsWith('.started')) ?? '';
			return cloneStartedEntry.length > 0;
		});
		const holdToken = cloneStartedEntry.slice(0, -'.started'.length);
		const cloneTarget = decodeURIComponent(holdToken);
		const cloneReleasePath = path.join(fakeGit.holdRoot!, `${holdToken}.release`);

		const service = createResourcesService(
			createConfigServiceMock({
				resourcesDirectory,
				resources: []
			})
		);

		const clearPromise = service.clearCachesPromise();
		await waitFor(() => clearBlocked);
		expect(await pathExists(cloneTarget)).toBe(true);

		await Bun.write(cloneReleasePath, '');

		const workerResult = await workerPromise;
		const clearResult = await clearPromise;

		expect(workerResult.exitCode).toBe(0);
		expect(clearResult.cleared).toBeGreaterThanOrEqual(1);
		expect(await pathExists(cloneTarget)).toBe(false);
	});

	it('clear waits for an in-flight named mirror materialization even before the mirror exists', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const fakeGit = await createFakeGitBinary(tempDir, { holdMutationsUntilRelease: true });
		const payload = {
			resourcesDirectory,
			name: 'docs',
			url: 'https://example.com/repo.git',
			branch: 'main'
		};
		const mirrorPath = getGitMirrorPath(resourcesDirectory, 'docs');
		const mirrorRepoPath = getGitMirrorRepoPath(resourcesDirectory, 'docs');
		const holdToken = encodeURIComponent(mirrorRepoPath);
		const cloneStartedPath = path.join(fakeGit.holdRoot!, `${holdToken}.started`);
		const cloneReleasePath = path.join(fakeGit.holdRoot!, `${holdToken}.release`);
		let clearBlocked = false;
		setFilesystemLockTestHookForTests((event) => {
			if (
				event.phase === 'waiting-on-live-lock' &&
				event.lockPath === getGitLockPath(resourcesDirectory, 'docs') &&
				event.label === 'clear.git.docs'
			) {
				clearBlocked = true;
			}
		});

		const workerPromise = runWorker({ ...fakeGit, payload });
		await waitFor(() => pathExists(cloneStartedPath));
		expect(await Bun.file(mirrorPath).exists()).toBe(false);

		const service = createResourcesService(
			createConfigServiceMock({
				resourcesDirectory,
				resources: [
					{
						type: 'git',
						name: 'docs',
						url: 'https://example.com/repo.git',
						branch: 'main'
					}
				]
			})
		);

		const clearPromise = service.clearCachesPromise();
		await waitFor(() => clearBlocked);
		await Bun.write(cloneReleasePath, '');

		const clearResult = await clearPromise;
		const workerResult = await workerPromise;

		expect(workerResult.exitCode).toBe(0);
		expect(clearResult.cleared).toBeGreaterThanOrEqual(1);
		expect(await Bun.file(mirrorPath).exists()).toBe(false);
	});

	it('named git materialization recovers after a stale clear.lock instead of hanging', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const fakeGit = await createFakeGitBinary(tempDir);
		await createStaleLockDirectory(path.join(resourcesDirectory, '.resource-locks', 'clear.lock'));

		const workerResult = await runWorker({
			...fakeGit,
			payload: {
				resourcesDirectory,
				name: 'docs',
				url: 'https://example.com/repo.git',
				branch: 'main'
			}
		});

		expect(workerResult.exitCode).toBe(0);
		expect(workerResult.stderr).toBe('');
	});

	it('named git materialization recovers after a stale clear.lock with a real local bare repo', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const remote = await createRealGitRemote(tempDir);
		await createStaleLockDirectory(path.join(resourcesDirectory, '.resource-locks', 'clear.lock'));

		const workerResult = await runWorker({
			env: remote.gitEnv,
			payload: {
				resourcesDirectory,
				name: 'docs',
				url: remote.remoteUrl,
				branch: 'main',
				repoSubPaths: ['docs']
			}
		});

		expect(workerResult.exitCode).toBe(0);
		expect(workerResult.stderr).toBe('');
	});
});
