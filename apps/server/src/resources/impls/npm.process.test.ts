import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { createAnonymousDirectoryKey, createResourcesService } from '../service.ts';
import { setFilesystemLockTestHookForTests, type FilesystemLockTestEvent } from '../lock.ts';
import { getNpmLockPath } from '../layout.ts';
import type { ResourceDefinition } from '../schema.ts';
import { createConfigServiceMock, createStaleLockDirectory } from '../test-support.ts';
import { logQueueMetrics, readStream, waitFor } from './test-fixtures/process-support.ts';

const workerPath = path.join(import.meta.dir, 'test-fixtures', 'npm-materialize-worker.ts');

type WorkerAction =
	| {
			readonly type: 'materialize';
			readonly reference: string;
			readonly destinationPath?: string;
	  }
	| {
			readonly type: 'cleanup';
			readonly reference: string;
	  }
	| {
			readonly type: 'clear';
	  };

type WorkerMockConfig = {
	readonly packageName: string;
	readonly resolvedVersion: string;
	readonly pageHtml?: string;
	readonly holdInstallUntilContinue?: boolean;
	readonly exitCode?: number;
	readonly stdout?: string;
	readonly stderr?: string;
	readonly exitBeforeResult?: boolean;
};

type WorkerConfig = {
	readonly resourcesDirectory: string;
	readonly resources: readonly ResourceDefinition[];
	readonly action: WorkerAction;
	readonly mock: WorkerMockConfig;
};

type ParentToChildMessage = {
	readonly type: 'continue-install';
};

type ChildToParentMessage =
	| {
			readonly type: 'install-started';
			readonly packageSpec: string;
			readonly cwd?: string;
	  }
	| {
			readonly type: 'lock-event';
			readonly event: FilesystemLockTestEvent;
	  }
	| {
			readonly type: 'result';
			readonly result: unknown;
	  }
	| {
			readonly type: 'error';
			readonly message: string;
	  };

type SpawnedWorker = {
	readonly process: ReturnType<typeof Bun.spawn>;
	readonly exitPromise: Promise<number>;
	readonly stdoutPromise: Promise<string>;
	readonly stderrPromise: Promise<string>;
	readonly messages: ReturnType<typeof createMessageBuffer>;
	readonly send: (message: ParentToChildMessage) => void;
	readonly hasExited: () => boolean;
};

const pathExists = async (targetPath: string) => {
	try {
		await fs.stat(targetPath);
		return true;
	} catch {
		return false;
	}
};

const createMessageBuffer = () => {
	const pendingMessages: ChildToParentMessage[] = [];
	const waiters = new Set<{
		readonly predicate: (message: ChildToParentMessage) => boolean;
		readonly resolve: (message: ChildToParentMessage) => void;
		readonly reject: (cause: Error) => void;
		readonly timer: ReturnType<typeof setTimeout>;
	}>();

	return {
		push(message: ChildToParentMessage) {
			for (const waiter of waiters) {
				if (!waiter.predicate(message)) continue;
				clearTimeout(waiter.timer);
				waiters.delete(waiter);
				waiter.resolve(message);
				return;
			}
			pendingMessages.push(message);
		},
		waitFor(
			predicate: (message: ChildToParentMessage) => boolean,
			timeoutMs = 4_000
		): Promise<ChildToParentMessage> {
			const existing = pendingMessages.find(predicate);
			if (existing) {
				pendingMessages.splice(pendingMessages.indexOf(existing), 1);
				return Promise.resolve(existing);
			}

			return new Promise((resolve, reject) => {
				const waiter = {
					predicate,
					resolve,
					reject,
					timer: setTimeout(() => {
						waiters.delete(waiter);
						reject(new Error('Timed out waiting for worker IPC message'));
					}, timeoutMs)
				};
				waiters.add(waiter);
			});
		}
	};
};

const spawnWorker = async (config: WorkerConfig): Promise<SpawnedWorker> => {
	const workerDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-npm-worker-'));
	const configPath = path.join(workerDir, 'worker-config.json');
	await Bun.write(configPath, JSON.stringify(config, null, 2));

	const messages = createMessageBuffer();
	let exitCode: number | null = null;
	const childProcess = Bun.spawn([process.execPath, workerPath, configPath], {
		stdout: 'pipe',
		stderr: 'pipe',
		serialization: 'json',
		ipc(message) {
			if (!message || typeof message !== 'object') return;
			const type = Reflect.get(message, 'type');
			if (
				type === 'install-started' ||
				type === 'lock-event' ||
				type === 'result' ||
				type === 'error'
			) {
				messages.push(message as ChildToParentMessage);
			}
		}
	});

	const stdoutPromise = readStream(childProcess.stdout);
	const stderrPromise = readStream(childProcess.stderr);
	const exitPromise = childProcess.exited.then(async (code) => {
		exitCode = code;
		await fs.rm(workerDir, { recursive: true, force: true });
		return code;
	});

	return {
		process: childProcess,
		exitPromise,
		stdoutPromise,
		stderrPromise,
		messages,
		send(message) {
			childProcess.send(message);
		},
		hasExited: () => exitCode !== null
	};
};

const waitForInstallStarted = async (worker: SpawnedWorker) => {
	const message = await Promise.race([
		worker.messages.waitFor((candidate) => candidate.type === 'install-started'),
		worker.exitPromise.then(() => 'exited' as const)
	]);

	if (message === 'exited') {
		const [exitCode, stdout, stderr] = await Promise.all([
			worker.exitPromise,
			worker.stdoutPromise,
			worker.stderrPromise
		]);
		throw new Error(
			`Worker exited ${exitCode} before sending install-started.\nSTDOUT:\n${stdout}\nSTDERR:\n${stderr}`
		);
	}

	if (message.type !== 'install-started') {
		throw new Error(`Expected install-started IPC message, received ${message.type}`);
	}

	return message;
};

const waitForWorkerLockEvent = async (
	worker: SpawnedWorker,
	predicate: (event: FilesystemLockTestEvent) => boolean,
	timeoutMs = 4_000
) => {
	const message = await worker.messages.waitFor(
		(candidate) => candidate.type === 'lock-event' && predicate(candidate.event),
		timeoutMs
	);
	if (message.type !== 'lock-event') {
		throw new Error(`Expected lock-event IPC message, received ${message.type}`);
	}
	return message.event;
};

const waitForWorker = async <T>(worker: SpawnedWorker): Promise<T> => {
	const terminalMessage = await Promise.race([
		worker.messages.waitFor(
			(candidate) => candidate.type === 'result' || candidate.type === 'error',
			5_000
		),
		worker.exitPromise.then(() => 'exited' as const)
	]);

	if (terminalMessage === 'exited') {
		const [exitCode, stdout, stderr] = await Promise.all([
			worker.exitPromise,
			worker.stdoutPromise,
			worker.stderrPromise
		]);
		throw new Error(
			`Worker exited ${exitCode} before sending a terminal IPC message.\nSTDOUT:\n${stdout}\nSTDERR:\n${stderr}`
		);
	}

	const [exitCode, stdout, stderr] = await Promise.all([
		worker.exitPromise,
		worker.stdoutPromise,
		worker.stderrPromise
	]);
	if (terminalMessage.type === 'error') {
		throw new Error(
			`Worker reported an error: ${terminalMessage.message}\nSTDOUT:\n${stdout}\nSTDERR:\n${stderr}`
		);
	}
	if (terminalMessage.type !== 'result') {
		throw new Error(`Expected result IPC message, received ${terminalMessage.type}`);
	}

	if (exitCode !== 0) {
		throw new Error(
			`Worker exited ${exitCode} after sending result.\nSTDOUT:\n${stdout}\nSTDERR:\n${stderr}`
		);
	}

	return terminalMessage.result as T;
};

describe('npm process locking', () => {
	let tempDir: string;

	beforeEach(async () => {
		tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-npm-process-'));
	});

	afterEach(async () => {
		setFilesystemLockTestHookForTests();
		await fs.rm(tempDir, { recursive: true, force: true });
	});

	it('measures same-resource queueing for concurrent named npm materialization', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const resourceName = 'react-docs';
		const packageName = 'react';
		const resourceLockPath = getNpmLockPath(resourcesDirectory, resourceName);
		const resources = [{ type: 'npm', name: resourceName, package: packageName }] as const;

		const startedAt = performance.now();
		const firstWorker = await spawnWorker({
			resourcesDirectory,
			resources,
			action: { type: 'materialize', reference: resourceName },
			mock: {
				packageName,
				resolvedVersion: '19.0.0',
				holdInstallUntilContinue: true
			}
		});

		await waitForInstallStarted(firstWorker);

		const queuedWorkerStartedAt = performance.now();
		const secondWorker = await spawnWorker({
			resourcesDirectory,
			resources,
			action: { type: 'materialize', reference: resourceName },
			mock: {
				packageName,
				resolvedVersion: '19.0.0',
				holdInstallUntilContinue: true
			}
		});

		await waitForWorkerLockEvent(
			secondWorker,
			(event) => event.phase === 'waiting-on-live-lock' && event.lockPath === resourceLockPath
		);

		firstWorker.send({ type: 'continue-install' });
		const firstResultPromise = waitForWorker<{ metadata: { package?: string } }>(firstWorker);
		const secondInstallStarted = await waitForInstallStarted(secondWorker);
		const queuedWorkerWaitMs = performance.now() - queuedWorkerStartedAt;

		expect(secondInstallStarted.type).toBe('install-started');

		secondWorker.send({ type: 'continue-install' });
		const [firstResult, secondResult] = await Promise.all([
			firstResultPromise,
			waitForWorker<{ metadata: { package?: string } }>(secondWorker)
		]);

		logQueueMetrics('npm.named.same-resource', {
			totalElapsedMs: performance.now() - startedAt,
			queuedWorkerWaitMs
		});

		expect(firstResult.metadata.package).toBe(packageName);
		expect(secondResult.metadata.package).toBe(packageName);
	});

	it('clear waits for an in-flight named npm materialization even before metadata exists', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const resourceName = 'react-docs';
		const packageName = 'react';
		const cachePath = path.join(resourcesDirectory, resourceName);
		const resourceLockPath = getNpmLockPath(resourcesDirectory, resourceName);
		let clearBlocked = false;
		setFilesystemLockTestHookForTests((event) => {
			if (
				event.phase === 'waiting-on-live-lock' &&
				event.lockPath === resourceLockPath &&
				event.label === `clear.npm.${resourceName}`
			) {
				clearBlocked = true;
			}
		});

		const worker = await spawnWorker({
			resourcesDirectory,
			resources: [{ type: 'npm', name: resourceName, package: packageName }],
			action: { type: 'materialize', reference: resourceName },
			mock: {
				packageName,
				resolvedVersion: '19.0.0',
				holdInstallUntilContinue: true
			}
		});

		await waitForInstallStarted(worker);
		await waitFor(() => pathExists(cachePath));
		expect(await pathExists(path.join(cachePath, '.btca-npm-meta.json'))).toBe(false);

		const service = createResourcesService(
			createConfigServiceMock({
				resourcesDirectory,
				resources: [{ type: 'npm', name: resourceName, package: packageName }]
			})
		);

		const clearPromise = service.clearCachesPromise();
		await waitFor(() => clearBlocked);

		worker.send({ type: 'continue-install' });
		const [workerResult, clearResult] = await Promise.all([
			waitForWorker<{ metadata: { package?: string } }>(worker),
			clearPromise
		]);

		expect(workerResult.metadata.package).toBe(packageName);
		expect(clearResult.cleared).toBeGreaterThanOrEqual(1);
		expect(await pathExists(cachePath)).toBe(false);
	});

	it('clear drains extra anonymous npm locks and removes tmp caches', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const reference = 'react';
		const resourceName = 'anonymous:npm:react';
		const anonymousKey = createAnonymousDirectoryKey(reference);
		const tmpCachePath = path.join(resourcesDirectory, '.tmp', anonymousKey);
		const resourceLockPath = getNpmLockPath(resourcesDirectory, anonymousKey);
		let clearBlocked = false;
		setFilesystemLockTestHookForTests((event) => {
			if (
				event.phase === 'waiting-on-live-lock' &&
				event.lockPath === resourceLockPath &&
				event.label === `clear.npm.${anonymousKey}`
			) {
				clearBlocked = true;
			}
		});

		const worker = await spawnWorker({
			resourcesDirectory,
			resources: [{ type: 'npm', name: resourceName, package: reference }],
			action: { type: 'materialize', reference: resourceName },
			mock: {
				packageName: reference,
				resolvedVersion: '19.0.0',
				holdInstallUntilContinue: true
			}
		});

		await waitForInstallStarted(worker);
		await waitFor(() => pathExists(tmpCachePath));

		const service = createResourcesService(
			createConfigServiceMock({
				resourcesDirectory,
				resources: []
			})
		);

		const clearPromise = service.clearCachesPromise();
		await waitFor(() => clearBlocked);

		worker.send({ type: 'continue-install' });
		const [workerResult, clearResult] = await Promise.all([
			waitForWorker<{ metadata: { package?: string } }>(worker),
			clearPromise
		]);

		expect(workerResult.metadata.package).toBe(reference);
		expect(clearResult.cleared).toBeGreaterThanOrEqual(1);
		expect(await pathExists(tmpCachePath)).toBe(false);
	});

	it('overlapping anonymous npm cleanup waits for live materialization before removing the tmp cache', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const reference = 'react';
		const resourceName = 'anonymous:npm:react';
		const anonymousKey = createAnonymousDirectoryKey(reference);
		const tmpCachePath = path.join(resourcesDirectory, '.tmp', anonymousKey);
		const resourceLockPath = getNpmLockPath(resourcesDirectory, anonymousKey);
		const resources = [{ type: 'npm', name: resourceName, package: reference }] as const;

		const materializeWorker = await spawnWorker({
			resourcesDirectory,
			resources,
			action: { type: 'materialize', reference: resourceName },
			mock: {
				packageName: reference,
				resolvedVersion: '19.0.0',
				holdInstallUntilContinue: true
			}
		});

		await waitForInstallStarted(materializeWorker);
		await waitFor(() => pathExists(tmpCachePath));

		const cleanupWorker = await spawnWorker({
			resourcesDirectory,
			resources,
			action: { type: 'cleanup', reference: resourceName },
			mock: {
				packageName: reference,
				resolvedVersion: '19.0.0'
			}
		});

		const cleanupPromise = waitForWorker<{ cleaned: boolean; name: string }>(cleanupWorker);
		await waitForWorkerLockEvent(
			cleanupWorker,
			(event) => event.phase === 'waiting-on-live-lock' && event.lockPath === resourceLockPath
		);
		expect(await pathExists(tmpCachePath)).toBe(true);

		materializeWorker.send({ type: 'continue-install' });
		const [materializeResult, cleanupResult] = await Promise.all([
			waitForWorker<{ metadata: { package?: string } }>(materializeWorker),
			cleanupPromise
		]);

		expect(materializeResult.metadata.package).toBe(reference);
		expect(cleanupResult.cleaned).toBe(true);
		expect(cleanupResult.name).toBe(resourceName);
		expect(await pathExists(tmpCachePath)).toBe(false);
	});

	it('clear keeps npm lock ordering even when config now reuses the key for git', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const resourceName = 'shared-key';
		const packageName = 'react';
		const cachePath = path.join(resourcesDirectory, resourceName);
		const metaPath = path.join(cachePath, '.btca-npm-meta.json');
		const resourceLockPath = getNpmLockPath(resourcesDirectory, resourceName);
		let clearBlocked = false;
		setFilesystemLockTestHookForTests((event) => {
			if (
				event.phase === 'waiting-on-live-lock' &&
				event.lockPath === resourceLockPath &&
				event.label === `clear.npm.${resourceName}`
			) {
				clearBlocked = true;
			}
		});

		const worker = await spawnWorker({
			resourcesDirectory,
			resources: [{ type: 'npm', name: resourceName, package: packageName }],
			action: { type: 'materialize', reference: resourceName },
			mock: {
				packageName,
				resolvedVersion: '19.0.0',
				holdInstallUntilContinue: true
			}
		});

		await waitForInstallStarted(worker);
		await waitFor(() => pathExists(cachePath));
		await Bun.write(
			metaPath,
			JSON.stringify(
				{
					packageName,
					resolvedVersion: '19.0.0',
					packageUrl: 'https://www.npmjs.com/package/react',
					pageUrl: 'https://www.npmjs.com/package/react/v/19.0.0',
					fetchedAt: new Date().toISOString()
				},
				null,
				2
			)
		);

		const service = createResourcesService(
			createConfigServiceMock({
				resourcesDirectory,
				resources: [
					{
						type: 'git',
						name: resourceName,
						url: 'https://example.com/repo.git',
						branch: 'main'
					}
				]
			})
		);

		const clearPromise = service.clearCachesPromise();
		await waitFor(() => clearBlocked);

		worker.send({ type: 'continue-install' });
		const [workerResult, clearResult] = await Promise.all([
			waitForWorker<{ metadata: { package?: string } }>(worker),
			clearPromise
		]);

		expect(workerResult.metadata.package).toBe(packageName);
		expect(clearResult.cleared).toBeGreaterThanOrEqual(1);
		expect(await pathExists(cachePath)).toBe(false);
	});

	it('named npm materialization recovers after a stale clear.lock instead of hanging', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		await createStaleLockDirectory(path.join(resourcesDirectory, '.resource-locks', 'clear.lock'));

		const worker = await spawnWorker({
			resourcesDirectory,
			resources: [{ type: 'npm', name: 'react-docs', package: 'react' }],
			action: { type: 'materialize', reference: 'react-docs' },
			mock: {
				packageName: 'react',
				resolvedVersion: '19.0.0'
			}
		});

		const result = await waitForWorker<{ metadata: { package?: string } }>(worker);
		expect(result.metadata.package).toBe('react');
	});

	it('surfaces stdout and stderr if the worker exits before sending a result', async () => {
		const resourcesDirectory = path.join(tempDir, 'resources');
		const worker = await spawnWorker({
			resourcesDirectory,
			resources: [{ type: 'npm', name: 'react-docs', package: 'react' }],
			action: { type: 'materialize', reference: 'react-docs' },
			mock: {
				packageName: 'react',
				resolvedVersion: '19.0.0',
				stdout: 'worker stdout',
				stderr: 'worker stderr',
				exitBeforeResult: true
			}
		});

		await expect(waitForWorker(worker)).rejects.toThrow(/worker stdout[\s\S]*worker stderr/);
	});
});
