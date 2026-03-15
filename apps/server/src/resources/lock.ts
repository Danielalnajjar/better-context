import { randomUUID } from 'node:crypto';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { setInterval as heartbeatTicks, setTimeout as sleep } from 'node:timers/promises';

export type FilesystemLockArgs = {
	readonly lockPath: string;
	readonly label: string;
	readonly pollMs?: number;
	readonly heartbeatMs?: number;
	readonly staleMs?: number;
	readonly quiet?: boolean;
};

export type ClearAwareFilesystemLockArgs = {
	readonly clearLockPath: string;
	readonly clearLockWaitLabel: string;
	readonly clearLockInspectLabel: string;
	readonly resourceLockPath: string;
	readonly resourceLockLabel: string;
	readonly quiet?: boolean;
};

export type FilesystemLockOwner = {
	readonly pid: number;
	readonly token: string;
	readonly label: string;
	readonly startedAt: string;
};

type ResolvedFilesystemLockArgs = Required<FilesystemLockArgs>;

type LockInstanceIdentity = {
	readonly dev: string;
	readonly ino: string;
	readonly mtimeMs: number;
};

type FreshnessSource = 'heartbeat' | 'startedAt' | 'lockMtime';

type LockObservation = {
	readonly status: 'absent' | 'live' | 'stale';
	readonly instance?: LockInstanceIdentity;
	readonly owner: FilesystemLockOwner | null;
	readonly freshnessSource?: FreshnessSource;
	readonly freshnessTimestampMs?: number;
};

type StaleBreakClaim = {
	readonly token: string;
	readonly claimedAt: string;
	readonly observedDev: string;
	readonly observedIno: string;
	readonly freshnessSource: FreshnessSource;
	readonly observedLockMtimeMs?: string;
};

export type FilesystemLockTestEvent =
	| {
			readonly phase: 'stale-break-before-claim-mkdir' | 'stale-break-after-claim-mkdir';
			readonly lockPath: string;
			readonly claimPath: string;
	  }
	| {
			readonly phase: 'waiting-on-live-lock';
			readonly lockPath: string;
			readonly label: string;
	  }
	| {
			readonly phase: 'clear-wait-started' | 'clear-wait-finished';
			readonly lockPath: string;
			readonly label: string;
	  }
	| {
			readonly phase: 'clear-aware-resource-lock-acquired' | 'clear-aware-retry-after-clear';
			readonly clearLockPath: string;
			readonly clearLockWaitLabel: string;
			readonly clearLockInspectLabel: string;
			readonly resourceLockPath: string;
			readonly resourceLockLabel: string;
	  };

const OWNER_FILE = 'owner.json';
const HEARTBEAT_FILE = 'heartbeat';
const STALE_BREAK_CLAIM_DIR = '.stale-break-claim';
const STALE_BREAK_CLAIM_FILE = 'claim.json';

let filesystemLockTestHookForTests:
	| ((event: FilesystemLockTestEvent) => void | Promise<void>)
	| undefined;

export const DEFAULT_LOCK_POLL_MS = 100;
export const DEFAULT_LOCK_HEARTBEAT_MS = 2000;
export const DEFAULT_LOCK_STALE_MS = 30000;

export const setFilesystemLockTestHookForTests = (
	hook?: (event: FilesystemLockTestEvent) => void | Promise<void>
) => {
	filesystemLockTestHookForTests = hook;
};

const emitFilesystemLockTestEvent = async (event: FilesystemLockTestEvent) => {
	await filesystemLockTestHookForTests?.(event);
};

const resolveLockArgs = (args: FilesystemLockArgs): ResolvedFilesystemLockArgs => {
	const resolved = {
		lockPath: args.lockPath,
		label: args.label,
		pollMs: args.pollMs ?? DEFAULT_LOCK_POLL_MS,
		heartbeatMs: args.heartbeatMs ?? DEFAULT_LOCK_HEARTBEAT_MS,
		staleMs: args.staleMs ?? DEFAULT_LOCK_STALE_MS,
		quiet: args.quiet ?? false
	};

	if (resolved.staleMs < resolved.heartbeatMs * 5) {
		throw new Error(
			`Invalid filesystem lock timings for "${resolved.label}": staleMs must be at least heartbeatMs * 5`
		);
	}

	return resolved;
};

const ensureLockParent = async (lockPath: string) => {
	await fs.mkdir(path.dirname(lockPath), { recursive: true });
};

const getHeartbeatPath = (lockPath: string) => path.join(lockPath, HEARTBEAT_FILE);
const getOwnerPath = (lockPath: string) => path.join(lockPath, OWNER_FILE);
const getClaimPath = (lockPath: string) => path.join(lockPath, STALE_BREAK_CLAIM_DIR);
const getClaimFilePath = (lockPath: string) =>
	path.join(getClaimPath(lockPath), STALE_BREAK_CLAIM_FILE);

const readJsonFile = async <T>(filePath: string): Promise<T | null> => {
	try {
		return JSON.parse(await Bun.file(filePath).text()) as T;
	} catch {
		return null;
	}
};

const safeStat = async (filePath: string) => {
	try {
		return await fs.stat(filePath);
	} catch {
		return null;
	}
};

const readLockInstanceIdentity = async (lockPath: string): Promise<LockInstanceIdentity | null> => {
	try {
		const stats = await fs.lstat(lockPath, { bigint: true });
		return {
			dev: stats.dev.toString(),
			ino: stats.ino.toString(),
			mtimeMs: Number(stats.mtimeMs)
		};
	} catch {
		return null;
	}
};

const sameInstance = (
	left?: Pick<LockInstanceIdentity, 'dev' | 'ino'> | null,
	right?: Pick<LockInstanceIdentity, 'dev' | 'ino'> | null
) => Boolean(left && right && left.dev === right.dev && left.ino === right.ino);

const isValidActiveResourceLockKey = (key: string) => {
	if (key.length === 0 || key === '.' || key === '..') return false;

	try {
		return encodeURIComponent(decodeURIComponent(key)) === key;
	} catch {
		return false;
	}
};

export const readLockOwner = async (lockPath: string): Promise<FilesystemLockOwner | null> => {
	const parsed = await readJsonFile<Partial<FilesystemLockOwner>>(getOwnerPath(lockPath));
	if (
		typeof parsed?.pid === 'number' &&
		typeof parsed.token === 'string' &&
		typeof parsed.label === 'string' &&
		typeof parsed.startedAt === 'string'
	) {
		return {
			pid: parsed.pid,
			token: parsed.token,
			label: parsed.label,
			startedAt: parsed.startedAt
		};
	}
	return null;
};

const readClaim = async (lockPath: string): Promise<StaleBreakClaim | null> => {
	const parsed = await readJsonFile<Partial<StaleBreakClaim>>(getClaimFilePath(lockPath));
	if (
		typeof parsed?.token === 'string' &&
		typeof parsed.claimedAt === 'string' &&
		typeof parsed.observedDev === 'string' &&
		typeof parsed.observedIno === 'string' &&
		(parsed.freshnessSource === 'heartbeat' ||
			parsed.freshnessSource === 'startedAt' ||
			parsed.freshnessSource === 'lockMtime')
	) {
		return {
			token: parsed.token,
			claimedAt: parsed.claimedAt,
			observedDev: parsed.observedDev,
			observedIno: parsed.observedIno,
			freshnessSource: parsed.freshnessSource,
			...(typeof parsed.observedLockMtimeMs === 'string'
				? { observedLockMtimeMs: parsed.observedLockMtimeMs }
				: {})
		};
	}
	return null;
};

const touchHeartbeat = async (lockPath: string) => {
	const heartbeatPath = getHeartbeatPath(lockPath);
	const now = new Date();
	try {
		await fs.utimes(heartbeatPath, now, now);
	} catch {
		await Bun.write(heartbeatPath, '');
	}
};

const observeLock = async (
	args: ResolvedFilesystemLockArgs,
	options?: { readonly fallbackLockMtimeMs?: number }
): Promise<LockObservation> => {
	const instance = await readLockInstanceIdentity(args.lockPath);
	if (!instance) {
		return { status: 'absent', owner: null };
	}

	const owner = await readLockOwner(args.lockPath);
	const heartbeat = await safeStat(getHeartbeatPath(args.lockPath));
	const now = Date.now();

	if (heartbeat) {
		if (now - heartbeat.mtimeMs < args.staleMs) {
			return {
				status: 'live',
				instance,
				owner,
				freshnessSource: 'heartbeat',
				freshnessTimestampMs: heartbeat.mtimeMs
			};
		}

		return {
			status: 'stale',
			instance,
			owner,
			freshnessSource: 'heartbeat',
			freshnessTimestampMs: heartbeat.mtimeMs
		};
	}

	if (owner) {
		const startedAtMs = Date.parse(owner.startedAt);
		const freshnessTimestampMs = Number.isFinite(startedAtMs)
			? startedAtMs
			: (options?.fallbackLockMtimeMs ?? instance.mtimeMs);

		if (now - freshnessTimestampMs < args.staleMs) {
			return {
				status: 'live',
				instance,
				owner,
				freshnessSource: Number.isFinite(startedAtMs) ? 'startedAt' : 'lockMtime',
				freshnessTimestampMs
			};
		}

		return {
			status: 'stale',
			instance,
			owner,
			freshnessSource: Number.isFinite(startedAtMs) ? 'startedAt' : 'lockMtime',
			freshnessTimestampMs
		};
	}

	const freshnessTimestampMs = options?.fallbackLockMtimeMs ?? instance.mtimeMs;
	return {
		status: now - freshnessTimestampMs < args.staleMs ? 'live' : 'stale',
		instance,
		owner: null,
		freshnessSource: 'lockMtime',
		freshnessTimestampMs
	};
};

const tryClearStaleClaim = async (args: ResolvedFilesystemLockArgs) => {
	const claimPath = getClaimPath(args.lockPath);
	const claim = await readClaim(args.lockPath);
	const stat = await safeStat(claimPath);
	if (!stat) return false;

	const claimedAtMs = claim ? Date.parse(claim.claimedAt) : Number.NaN;
	const freshnessTimestampMs = Number.isFinite(claimedAtMs) ? claimedAtMs : stat.mtimeMs;
	if (Date.now() - freshnessTimestampMs < args.staleMs) return false;

	await fs.rm(claimPath, { recursive: true, force: true });
	return true;
};

const releaseClaimIfOwned = async (args: ResolvedFilesystemLockArgs, token: string) => {
	const claim = await readClaim(args.lockPath);
	if (!claim || claim.token !== token) return;
	await fs.rm(getClaimPath(args.lockPath), { recursive: true, force: true });
};

const tryBreakStaleLock = async (
	args: ResolvedFilesystemLockArgs,
	observation: LockObservation
): Promise<boolean> => {
	if (observation.status !== 'stale' || !observation.instance || !observation.freshnessSource) {
		return false;
	}

	const claimPath = getClaimPath(args.lockPath);
	const claimToken = randomUUID();
	await emitFilesystemLockTestEvent({
		phase: 'stale-break-before-claim-mkdir',
		lockPath: args.lockPath,
		claimPath
	});
	try {
		await fs.mkdir(claimPath);
	} catch (cause) {
		const code =
			typeof cause === 'object' && cause && 'code' in cause
				? (cause as { code?: string }).code
				: '';
		if (code === 'EEXIST') {
			return tryClearStaleClaim(args);
		}
		if (code === 'ENOENT') {
			return true;
		}
		throw cause;
	}

	await emitFilesystemLockTestEvent({
		phase: 'stale-break-after-claim-mkdir',
		lockPath: args.lockPath,
		claimPath
	});

	const claim: StaleBreakClaim = {
		token: claimToken,
		claimedAt: new Date().toISOString(),
		observedDev: observation.instance.dev,
		observedIno: observation.instance.ino,
		freshnessSource: observation.freshnessSource,
		...(observation.freshnessSource === 'lockMtime'
			? {
					observedLockMtimeMs: String(
						observation.freshnessTimestampMs ?? observation.instance.mtimeMs
					)
				}
			: {})
	};

	await Bun.write(getClaimFilePath(args.lockPath), JSON.stringify(claim, null, 2));

	const currentInstance = await readLockInstanceIdentity(args.lockPath);
	if (!sameInstance(currentInstance, observation.instance)) {
		await releaseClaimIfOwned(args, claimToken);
		return true;
	}

	const rechecked = await observeLock(args, {
		fallbackLockMtimeMs:
			observation.freshnessSource === 'lockMtime'
				? (observation.freshnessTimestampMs ?? observation.instance.mtimeMs)
				: undefined
	});
	if (rechecked.status !== 'stale' || !sameInstance(rechecked.instance, observation.instance)) {
		await releaseClaimIfOwned(args, claimToken);
		return false;
	}

	await fs.rm(args.lockPath, { recursive: true, force: true });
	return true;
};

const waitUntilLockClears = async (args: ResolvedFilesystemLockArgs) => {
	while (true) {
		const observation = await observeLock(args);
		if (observation.status === 'absent') return;
		const changed = await tryBreakStaleLock(args, observation);
		if (!changed) {
			if (observation.status === 'live') {
				await emitFilesystemLockTestEvent({
					phase: 'waiting-on-live-lock',
					lockPath: args.lockPath,
					label: args.label
				});
			}
			await sleep(args.pollMs);
		}
	}
};

export const clearFilesystemLockIfAbsentOrStale = async (
	args: FilesystemLockArgs
): Promise<boolean> => {
	const resolved = resolveLockArgs(args);
	await ensureLockParent(resolved.lockPath);

	while (true) {
		const observation = await observeLock(resolved);
		if (observation.status === 'absent') return true;
		if (observation.status === 'live') return false;

		const changed = await tryBreakStaleLock(resolved, observation);
		if (!changed) return false;
	}
};

const releaseOwnedLock = async (lockPath: string, owner: FilesystemLockOwner) => {
	const currentOwner = await readLockOwner(lockPath);
	if (!currentOwner || currentOwner.token !== owner.token) return;
	await fs.rm(lockPath, { recursive: true, force: true });
};

const startHeartbeatLoop = (lockPath: string, heartbeatMs: number) => {
	const controller = new AbortController();
	const completed = (async () => {
		try {
			for await (const _ of heartbeatTicks(heartbeatMs, undefined, {
				signal: controller.signal
			})) {
				try {
					await touchHeartbeat(lockPath);
				} catch {}
			}
		} catch (cause) {
			const name =
				typeof cause === 'object' && cause && 'name' in cause
					? String((cause as { name?: unknown }).name)
					: '';
			if (name !== 'AbortError') {
				return;
			}
		}
	})();

	return {
		stop: async () => {
			controller.abort();
			await completed;
		}
	};
};

export const withFilesystemLock = async <T>(
	args: FilesystemLockArgs,
	use: () => Promise<T>
): Promise<T> => {
	const resolved = resolveLockArgs(args);
	await ensureLockParent(resolved.lockPath);

	while (true) {
		try {
			await fs.mkdir(resolved.lockPath);
		} catch (cause) {
			const code =
				typeof cause === 'object' && cause && 'code' in cause
					? (cause as { code?: string }).code
					: '';
			if (code === 'EEXIST') {
				await waitUntilLockClears(resolved);
				continue;
			}
			throw cause;
		}

		const owner: FilesystemLockOwner = {
			pid: process.pid,
			token: randomUUID(),
			label: resolved.label,
			startedAt: new Date().toISOString()
		};
		try {
			await Bun.write(getOwnerPath(resolved.lockPath), JSON.stringify(owner, null, 2));
			await touchHeartbeat(resolved.lockPath);
		} catch (cause) {
			await fs.rm(resolved.lockPath, { recursive: true, force: true }).catch(() => undefined);
			throw cause;
		}

		const heartbeatLoop = startHeartbeatLoop(resolved.lockPath, resolved.heartbeatMs);

		try {
			return await use();
		} finally {
			await heartbeatLoop.stop();
			await releaseOwnedLock(resolved.lockPath, owner);
		}
	}
};

export const waitForFilesystemLockToClear = async (args: FilesystemLockArgs): Promise<void> => {
	const resolved = resolveLockArgs(args);
	await ensureLockParent(resolved.lockPath);
	await waitUntilLockClears(resolved);
};

export const withClearAwareFilesystemLock = async <T>(
	args: ClearAwareFilesystemLockArgs,
	use: () => Promise<T>
): Promise<T> => {
	while (true) {
		await emitFilesystemLockTestEvent({
			phase: 'clear-wait-started',
			lockPath: args.clearLockPath,
			label: args.clearLockWaitLabel
		});
		await waitForFilesystemLockToClear({
			lockPath: args.clearLockPath,
			label: args.clearLockWaitLabel,
			quiet: args.quiet
		});
		await emitFilesystemLockTestEvent({
			phase: 'clear-wait-finished',
			lockPath: args.clearLockPath,
			label: args.clearLockWaitLabel
		});

		const retryAfterClear = Symbol('retry-after-clear');
		const result = await withFilesystemLock(
			{
				lockPath: args.resourceLockPath,
				label: args.resourceLockLabel,
				quiet: args.quiet
			},
			async () => {
				await emitFilesystemLockTestEvent({
					phase: 'clear-aware-resource-lock-acquired',
					clearLockPath: args.clearLockPath,
					clearLockWaitLabel: args.clearLockWaitLabel,
					clearLockInspectLabel: args.clearLockInspectLabel,
					resourceLockPath: args.resourceLockPath,
					resourceLockLabel: args.resourceLockLabel
				});
				const clearLockIsGone = await clearFilesystemLockIfAbsentOrStale({
					lockPath: args.clearLockPath,
					label: args.clearLockInspectLabel,
					quiet: args.quiet
				});
				if (!clearLockIsGone) {
					await emitFilesystemLockTestEvent({
						phase: 'clear-aware-retry-after-clear',
						clearLockPath: args.clearLockPath,
						clearLockWaitLabel: args.clearLockWaitLabel,
						clearLockInspectLabel: args.clearLockInspectLabel,
						resourceLockPath: args.resourceLockPath,
						resourceLockLabel: args.resourceLockLabel
					});
					return retryAfterClear;
				}

				return use();
			}
		);

		if (result === retryAfterClear) continue;
		return result;
	}
};

export const parseActiveResourceLockDirectoryName = (
	entryName: string
): { readonly namespace: 'git' | 'npm'; readonly key: string } | null => {
	if (entryName.startsWith('git.') && entryName.endsWith('.lock')) {
		const key = entryName.slice('git.'.length, -'.lock'.length);
		if (!isValidActiveResourceLockKey(key)) return null;
		return {
			namespace: 'git',
			key
		};
	}
	if (entryName.startsWith('npm.') && entryName.endsWith('.lock')) {
		const key = entryName.slice('npm.'.length, -'.lock'.length);
		if (!isValidActiveResourceLockKey(key)) return null;
		return {
			namespace: 'npm',
			key
		};
	}
	return null;
};
