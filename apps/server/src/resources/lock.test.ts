import { afterEach, beforeEach, describe, expect, it, jest } from 'bun:test';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

import {
	clearFilesystemLockIfAbsentOrStale,
	parseActiveResourceLockDirectoryName,
	setFilesystemLockTestHookForTests,
	waitForFilesystemLockToClear,
	withClearAwareFilesystemLock,
	withFilesystemLock
} from './lock.ts';

const writeOwner = async (lockPath: string, args: { pid: number; startedAt: string }) => {
	await Bun.write(
		path.join(lockPath, 'owner.json'),
		JSON.stringify(
			{
				pid: args.pid,
				token: `token-${args.pid}`,
				label: 'test-lock',
				startedAt: args.startedAt
			},
			null,
			2
		)
	);
};

const waitFor = async (predicate: () => boolean | Promise<boolean>, timeoutMs = 2000) => {
	const startedAt = performance.now();
	while (performance.now() - startedAt < timeoutMs) {
		if (await predicate()) return;
		await sleep(10);
	}
	throw new Error('Timed out waiting for condition');
};

const createDeferred = <T = void>() => {
	let resolve!: (value: T | PromiseLike<T>) => void;
	const promise = new Promise<T>((innerResolve) => {
		resolve = innerResolve;
	});
	return { promise, resolve };
};

describe('resource lock helper', () => {
	let tempDir: string;

	beforeEach(async () => {
		tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-lock-test-'));
	});

	afterEach(async () => {
		setFilesystemLockTestHookForTests();
		jest.useRealTimers();
		await fs.rm(tempDir, { recursive: true, force: true });
	});

	it('parses active resource lock directory names without dot splitting', () => {
		expect(parseActiveResourceLockDirectoryName('git.docs.v1.lock')).toEqual({
			namespace: 'git',
			key: 'docs.v1'
		});
		expect(parseActiveResourceLockDirectoryName('npm.%40scope%2Fpkg.lock')).toEqual({
			namespace: 'npm',
			key: '%40scope%2Fpkg'
		});
		expect(parseActiveResourceLockDirectoryName('npm..lock')).toBeNull();
		expect(parseActiveResourceLockDirectoryName('npm....lock')).toBeNull();
		expect(parseActiveResourceLockDirectoryName('git.foo bar.lock')).toBeNull();
		expect(parseActiveResourceLockDirectoryName('clear.lock')).toBeNull();
	});

	it('does not steal a fresh lock with a missing owner file when heartbeat is fresh', async () => {
		const lockPath = path.join(tempDir, 'git.docs.lock');
		await fs.mkdir(lockPath, { recursive: true });
		await Bun.write(path.join(lockPath, 'heartbeat'), '');

		let acquired = false;
		const contender = withFilesystemLock(
			{
				lockPath,
				label: 'fresh-heartbeat',
				pollMs: 10,
				heartbeatMs: 20,
				staleMs: 100
			},
			async () => {
				acquired = true;
			}
		);

		let sawWaitOnFreshLock = false;
		setFilesystemLockTestHookForTests((event) => {
			if (event.phase === 'waiting-on-live-lock' && event.lockPath === lockPath) {
				sawWaitOnFreshLock = true;
			}
		});

		await waitFor(() => sawWaitOnFreshLock);
		expect(acquired).toBe(false);
		await fs.rm(lockPath, { recursive: true, force: true });
		await contender;
		expect(acquired).toBe(true);
	});

	it('treats owner-without-heartbeat as live while fresh and recoverable when stale with a dead pid', async () => {
		const freshLockPath = path.join(tempDir, 'fresh-pre-heartbeat.lock');
		await fs.mkdir(freshLockPath, { recursive: true });
		await writeOwner(freshLockPath, {
			pid: process.pid,
			startedAt: new Date().toISOString()
		});

		let freshAcquired = false;
		const freshContender = withFilesystemLock(
			{
				lockPath: freshLockPath,
				label: 'fresh-pre-heartbeat',
				pollMs: 10,
				heartbeatMs: 20,
				staleMs: 100
			},
			async () => {
				freshAcquired = true;
			}
		);

		let sawFreshWait = false;
		setFilesystemLockTestHookForTests((event) => {
			if (event.phase === 'waiting-on-live-lock' && event.lockPath === freshLockPath) {
				sawFreshWait = true;
			}
		});

		await waitFor(() => sawFreshWait);
		expect(freshAcquired).toBe(false);
		await fs.rm(freshLockPath, { recursive: true, force: true });
		await freshContender;
		expect(freshAcquired).toBe(true);

		const staleLockPath = path.join(tempDir, 'stale-pre-heartbeat.lock');
		await fs.mkdir(staleLockPath, { recursive: true });
		const staleStartedAt = new Date(Date.now() - 5_000).toISOString();
		await writeOwner(staleLockPath, {
			pid: 999999,
			startedAt: staleStartedAt
		});

		let staleAcquired = false;
		await withFilesystemLock(
			{
				lockPath: staleLockPath,
				label: 'stale-pre-heartbeat',
				pollMs: 10,
				heartbeatMs: 20,
				staleMs: 100
			},
			async () => {
				staleAcquired = true;
			}
		);
		expect(staleAcquired).toBe(true);
	});

	it('reclaims a stale owner-without-heartbeat lock even if the pid is currently live', async () => {
		const lockPath = path.join(tempDir, 'stale-live-pid-pre-heartbeat.lock');
		await fs.mkdir(lockPath, { recursive: true });
		await writeOwner(lockPath, {
			pid: process.pid,
			startedAt: new Date(Date.now() - 5_000).toISOString()
		});

		await waitForFilesystemLockToClear({
			lockPath,
			label: 'stale-live-pid-pre-heartbeat',
			pollMs: 10,
			heartbeatMs: 20,
			staleMs: 100
		});

		const exists = await fs
			.stat(lockPath)
			.then(() => true)
			.catch(() => false);
		expect(exists).toBe(false);
	});

	it('reclaims a stale heartbeat lock even if the pid is currently live', async () => {
		const lockPath = path.join(tempDir, 'stale-live-pid-heartbeat.lock');
		await fs.mkdir(lockPath, { recursive: true });
		await writeOwner(lockPath, {
			pid: process.pid,
			startedAt: new Date().toISOString()
		});
		const heartbeatPath = path.join(lockPath, 'heartbeat');
		await Bun.write(heartbeatPath, '');
		const oldDate = new Date(Date.now() - 5_000);
		await fs.utimes(heartbeatPath, oldDate, oldDate);
		await fs.utimes(lockPath, oldDate, oldDate);

		await waitForFilesystemLockToClear({
			lockPath,
			label: 'stale-live-pid-heartbeat',
			pollMs: 10,
			heartbeatMs: 20,
			staleMs: 100
		});

		const exists = await fs
			.stat(lockPath)
			.then(() => true)
			.catch(() => false);
		expect(exists).toBe(false);
	});

	it('reclaims a stale lock with no usable metadata', async () => {
		const lockPath = path.join(tempDir, 'damaged.lock');
		await fs.mkdir(lockPath, { recursive: true });
		const oldDate = new Date(Date.now() - 5_000);
		await fs.utimes(lockPath, oldDate, oldDate);

		let acquired = false;
		await withFilesystemLock(
			{
				lockPath,
				label: 'damaged-lock',
				pollMs: 10,
				heartbeatMs: 20,
				staleMs: 100
			},
			async () => {
				acquired = true;
			}
		);
		expect(acquired).toBe(true);
	});

	it('does not delete a fresh lock that replaced a stale one during stale-break handling', async () => {
		const lockPath = path.join(tempDir, 'aba.lock');
		await fs.mkdir(lockPath, { recursive: true });
		const oldDate = new Date(Date.now() - 5_000);
		await fs.utimes(lockPath, oldDate, oldDate);

		let swapped = false;
		setFilesystemLockTestHookForTests(async (event) => {
			if (
				!swapped &&
				event.phase === 'stale-break-after-claim-mkdir' &&
				event.lockPath === lockPath
			) {
				swapped = true;
				await fs.rm(lockPath, { recursive: true, force: true });
				await fs.mkdir(lockPath, { recursive: true });
				await writeOwner(lockPath, {
					pid: process.pid,
					startedAt: new Date().toISOString()
				});
				await Bun.write(path.join(lockPath, 'heartbeat'), '');
			}
		});

		let acquired = false;
		const contender = withFilesystemLock(
			{
				lockPath,
				label: 'aba-contender',
				pollMs: 10,
				heartbeatMs: 20,
				staleMs: 100
			},
			async () => {
				acquired = true;
			}
		);

		try {
			await waitFor(() => swapped);
			expect(swapped).toBe(true);
			expect(acquired).toBe(false);
			const freshLockStillExists = await fs
				.stat(lockPath)
				.then(() => true)
				.catch(() => false);
			expect(freshLockStillExists).toBe(true);

			await fs.rm(lockPath, { recursive: true, force: true });
			await contender;
			expect(acquired).toBe(true);
		} finally {
			setFilesystemLockTestHookForTests();
		}
	});

	it('retries cleanly if a stale lock disappears before the stale-break claim mkdir runs', async () => {
		const lockPath = path.join(tempDir, 'claim-parent-vanishes.lock');
		await fs.mkdir(lockPath, { recursive: true });
		const oldDate = new Date(Date.now() - 5_000);
		await fs.utimes(lockPath, oldDate, oldDate);

		let removedBeforeClaim = false;
		setFilesystemLockTestHookForTests(async (event) => {
			if (
				!removedBeforeClaim &&
				event.phase === 'stale-break-before-claim-mkdir' &&
				event.lockPath === lockPath
			) {
				removedBeforeClaim = true;
				await fs.rm(lockPath, { recursive: true, force: true });
			}
		});

		let acquired = false;
		try {
			await withFilesystemLock(
				{
					lockPath,
					label: 'claim-parent-vanishes',
					pollMs: 10,
					heartbeatMs: 20,
					staleMs: 100
				},
				async () => {
					acquired = true;
				}
			);
		} finally {
			setFilesystemLockTestHookForTests();
		}

		expect(removedBeforeClaim).toBe(true);
		expect(acquired).toBe(true);
	});

	it('keeps a fresh winner lock intact when two contenders race on the same stale lock', async () => {
		const lockPath = path.join(tempDir, 'two-contenders.lock');
		await fs.mkdir(lockPath, { recursive: true });
		const oldDate = new Date(Date.now() - 5_000);
		await fs.utimes(lockPath, oldDate, oldDate);

		let claimCreated = false;
		let releaseClaimCreation: () => void = () => {};
		const allowClaimCreationToContinue = new Promise<void>((resolve) => {
			releaseClaimCreation = resolve;
		});

		setFilesystemLockTestHookForTests(async (event) => {
			if (
				!claimCreated &&
				event.phase === 'stale-break-after-claim-mkdir' &&
				event.lockPath === lockPath
			) {
				claimCreated = true;
				await allowClaimCreationToContinue;
			}
		});

		let firstAcquired = false;
		let secondAcquired = false;
		let releaseFirst: () => void = () => {};
		const firstMayFinish = new Promise<void>((resolve) => {
			releaseFirst = resolve;
		});
		const first = withFilesystemLock(
			{
				lockPath,
				label: 'first-contender',
				pollMs: 10,
				heartbeatMs: 20,
				staleMs: 100
			},
			async () => {
				firstAcquired = true;
				await firstMayFinish;
			}
		);

		try {
			await waitFor(() => claimCreated);
			const second = withFilesystemLock(
				{
					lockPath,
					label: 'second-contender',
					pollMs: 10,
					heartbeatMs: 20,
					staleMs: 100
				},
				async () => {
					secondAcquired = true;
				}
			);

			releaseClaimCreation();
			await waitFor(() => firstAcquired);

			expect(secondAcquired).toBe(false);
			const freshLockStillExists = await fs
				.stat(lockPath)
				.then(() => true)
				.catch(() => false);
			expect(freshLockStillExists).toBe(true);

			releaseFirst();
			await first;
			await second;
			expect(secondAcquired).toBe(true);
		} finally {
			releaseFirst();
			releaseClaimCreation();
			setFilesystemLockTestHookForTests();
		}
	});

	it('clears stale locks while passively waiting for them to disappear', async () => {
		const lockPath = path.join(tempDir, 'wait-clear.lock');
		await fs.mkdir(lockPath, { recursive: true });
		const oldDate = new Date(Date.now() - 5_000);
		await fs.utimes(lockPath, oldDate, oldDate);

		await waitForFilesystemLockToClear({
			lockPath,
			label: 'wait-clear',
			pollMs: 10,
			heartbeatMs: 20,
			staleMs: 100
		});

		const exists = await fs
			.stat(lockPath)
			.then(() => true)
			.catch(() => false);
		expect(exists).toBe(false);
	});

	it('returns true when clearFilesystemLockIfAbsentOrStale sees no lock', async () => {
		const lockPath = path.join(tempDir, 'absent.lock');

		await expect(
			clearFilesystemLockIfAbsentOrStale({
				lockPath,
				label: 'absent-lock',
				pollMs: 10,
				heartbeatMs: 20,
				staleMs: 100
			})
		).resolves.toBe(true);
	});

	it('returns false when clearFilesystemLockIfAbsentOrStale sees a fresh live lock', async () => {
		const lockPath = path.join(tempDir, 'live.lock');
		const holderReady = createDeferred();
		const releaseHolder = createDeferred();

		const holder = withFilesystemLock(
			{
				lockPath,
				label: 'live-lock',
				pollMs: 10,
				heartbeatMs: 20,
				staleMs: 100
			},
			async () => {
				holderReady.resolve();
				await releaseHolder.promise;
			}
		);

		try {
			await holderReady.promise;
			await expect(
				clearFilesystemLockIfAbsentOrStale({
					lockPath,
					label: 'live-lock-clear',
					pollMs: 10,
					heartbeatMs: 20,
					staleMs: 100
				})
			).resolves.toBe(false);
		} finally {
			releaseHolder.resolve();
			await holder;
		}
	});

	it('reclaims a stale lock when clearFilesystemLockIfAbsentOrStale is called directly', async () => {
		const lockPath = path.join(tempDir, 'stale-clear.lock');
		await fs.mkdir(lockPath, { recursive: true });
		await writeOwner(lockPath, {
			pid: 999999,
			startedAt: new Date(Date.now() - 5_000).toISOString()
		});

		await expect(
			clearFilesystemLockIfAbsentOrStale({
				lockPath,
				label: 'stale-clear',
				pollMs: 10,
				heartbeatMs: 20,
				staleMs: 100
			})
		).resolves.toBe(true);

		const exists = await fs
			.stat(lockPath)
			.then(() => true)
			.catch(() => false);
		expect(exists).toBe(false);
	});

	it('retries clear-aware locking once and executes the callback only after the clear lock is gone', async () => {
		jest.useFakeTimers();

		const clearLockPath = path.join(tempDir, 'clear.lock');
		const resourceLockPath = path.join(tempDir, 'git.docs.lock');
		const acquiredAfterWait = createDeferred();
		const retryObserved = createDeferred();
		let resourceLockAcquireCount = 0;
		let clearLockInjected = false;
		let callbackCount = 0;

		setFilesystemLockTestHookForTests(async (event) => {
			if (
				event.phase === 'clear-aware-resource-lock-acquired' &&
				event.resourceLockPath === resourceLockPath
			) {
				resourceLockAcquireCount += 1;
				if (!clearLockInjected) {
					clearLockInjected = true;
					await fs.mkdir(clearLockPath, { recursive: true });
					await Bun.write(
						path.join(clearLockPath, 'owner.json'),
						JSON.stringify(
							{
								pid: process.pid,
								token: 'clear-lock-test',
								label: 'clear-lock-test',
								startedAt: new Date().toISOString()
							},
							null,
							2
						)
					);
					await Bun.write(path.join(clearLockPath, 'heartbeat'), '');
				} else {
					acquiredAfterWait.resolve();
				}
			}

			if (
				event.phase === 'clear-aware-retry-after-clear' &&
				event.resourceLockPath === resourceLockPath
			) {
				retryObserved.resolve();
			}
		});

		try {
			const resultPromise = withClearAwareFilesystemLock(
				{
					clearLockPath,
					clearLockWaitLabel: 'wait-clear-before-git.docs',
					clearLockInspectLabel: 'inspect-clear-before-git.docs',
					resourceLockPath,
					resourceLockLabel: 'git.docs',
					quiet: true
				},
				async () => {
					callbackCount += 1;
					return 'done';
				}
			);

			await retryObserved.promise;
			expect(callbackCount).toBe(0);

			await fs.rm(clearLockPath, { recursive: true, force: true });
			jest.advanceTimersByTime(120);
			await acquiredAfterWait.promise;

			await expect(resultPromise).resolves.toBe('done');
			expect(resourceLockAcquireCount).toBe(2);
			expect(callbackCount).toBe(1);
		} finally {
			setFilesystemLockTestHookForTests();
			jest.useRealTimers();
		}
	});
});
