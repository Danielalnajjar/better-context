import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { loadGitResource, type GitResourceDeps } from './git.ts';
import { getTmpCacheRoot } from '../layout.ts';
import { setFilesystemLockTestHookForTests } from '../lock.ts';
import type { BtcaGitResourceArgs } from '../types.ts';
import {
	createVirtualFs,
	disposeVirtualFs,
	existsInVirtualFs,
	readVirtualFsFile
} from '../../vfs/virtual-fs.ts';

type MockCommandLogEntry = {
	readonly cmd: string;
	readonly repoPath?: string;
	readonly branch?: string;
	readonly repoUrl?: string;
	readonly repoSubPaths?: readonly string[];
};

const pathExists = async (targetPath: string) => {
	try {
		await fs.stat(targetPath);
		return true;
	} catch {
		return false;
	}
};

const streamFromString = (value: string) =>
	new ReadableStream<Uint8Array>({
		start(controller) {
			if (value.length > 0) controller.enqueue(new TextEncoder().encode(value));
			controller.close();
		}
	});

const getMockFilesForUrl = (repoUrl: string) => ({
	'README.md': repoUrl.includes('repo-b') ? '# repo-b\n' : '# repo-a\n',
	'docs/guide.md': repoUrl.includes('repo-b') ? 'repo-b docs\n' : 'repo-a docs\n',
	'src/runtime.js': repoUrl.includes('repo-b')
		? `export const runtime = 'repo-b';\n`
		: `export const runtime = 'repo-a';\n`
});

const mockStatePaths = (repoPath: string) => ({
	origin: path.join(repoPath, '.btca-origin'),
	branch: path.join(repoPath, '.btca-branch'),
	head: path.join(repoPath, '.btca-head'),
	sparse: path.join(repoPath, '.git', 'info', 'sparse-checkout')
});

const writeCommandLog = async (logPath: string, entry: MockCommandLogEntry) => {
	await fs.appendFile(logPath, `${JSON.stringify(entry)}\n`);
};

const writeRepoState = async (repoPath: string, repoUrl: string, branch: string) => {
	const statePaths = mockStatePaths(repoPath);
	await fs.mkdir(path.join(repoPath, '.git', 'info'), { recursive: true });
	await fs.writeFile(path.join(repoPath, '.git', 'config'), '[mock]\n');
	await fs.writeFile(statePaths.origin, repoUrl);
	await fs.writeFile(statePaths.branch, branch);
	await fs.writeFile(statePaths.head, `commit:${repoUrl}:${branch}\n`);
};

const readRepoOrigin = async (repoPath: string) =>
	await Bun.file(mockStatePaths(repoPath).origin)
		.text()
		.catch(() => '');

const readRepoBranch = async (repoPath: string) =>
	await Bun.file(mockStatePaths(repoPath).branch)
		.text()
		.catch(() => '');

const removeWorkingTreeFiles = async (repoPath: string) => {
	let entries: string[] = [];
	try {
		entries = await fs.readdir(repoPath);
	} catch {
		entries = [];
	}

	for (const entry of entries) {
		if (entry === '.git' || entry.startsWith('.btca-')) continue;
		await fs.rm(path.join(repoPath, entry), { recursive: true, force: true });
	}
};

const applyWorkingTree = async (
	repoPath: string,
	repoUrl: string,
	repoSubPaths: readonly string[] | null
) => {
	await removeWorkingTreeFiles(repoPath);
	const allFiles = getMockFilesForUrl(repoUrl);
	const entries = Object.entries(allFiles).filter(([relativePath]) => {
		if (!repoSubPaths || repoSubPaths.length === 0) return true;
		return repoSubPaths.some(
			(repoSubPath) => relativePath === repoSubPath || relativePath.startsWith(`${repoSubPath}/`)
		);
	});

	for (const [relativePath, content] of entries) {
		const targetPath = path.join(repoPath, relativePath);
		await fs.mkdir(path.dirname(targetPath), { recursive: true });
		await fs.writeFile(targetPath, content);
	}

	const sparseFilePath = mockStatePaths(repoPath).sparse;
	if (repoSubPaths && repoSubPaths.length > 0) {
		await fs.mkdir(path.dirname(sparseFilePath), { recursive: true });
		await fs.writeFile(sparseFilePath, `${repoSubPaths.join('\n')}\n`);
	} else {
		await fs.rm(sparseFilePath, { force: true });
	}
};

const createGitSpawnMock = (logPath: string) =>
	((...spawnArgs: Parameters<typeof Bun.spawn>) => {
		const [command, options] = spawnArgs;
		const commandArgs = Array.isArray(command) ? command : [command];
		const gitArgs = commandArgs.slice(1);

		const exited = (async () => {
			const cwd = options?.cwd;

			if (gitArgs[0] === 'clone') {
				const repoPath = gitArgs.at(-1);
				const repoUrl = gitArgs.at(-2);
				const branchIndex = gitArgs.indexOf('-b');
				const branch = branchIndex >= 0 ? gitArgs[branchIndex + 1] : 'main';
				const sparseClone = gitArgs.includes('--sparse');
				if (!repoPath || !repoUrl) return 1;
				const resolvedBranch = branch ?? 'main';

				await writeCommandLog(logPath, {
					cmd: 'clone',
					repoPath,
					branch: resolvedBranch,
					repoUrl
				});
				await writeRepoState(repoPath, repoUrl, resolvedBranch);
				await applyWorkingTree(repoPath, repoUrl, sparseClone ? ['__pending_sparse__'] : null);
				if (sparseClone) {
					await removeWorkingTreeFiles(repoPath);
				}
				return 0;
			}

			if (gitArgs[0] === 'fetch') {
				await writeCommandLog(logPath, { cmd: 'fetch', repoPath: cwd ?? undefined });
				return 0;
			}

			if (gitArgs[0] === 'reset') {
				await writeCommandLog(logPath, { cmd: 'reset', repoPath: cwd ?? undefined });
				return 0;
			}

			if (gitArgs[0] === 'sparse-checkout' && gitArgs[1] === 'set') {
				if (!cwd) return 1;
				const repoUrl = await readRepoOrigin(cwd);
				const repoSubPaths = gitArgs.slice(2);
				await writeCommandLog(logPath, {
					cmd: 'sparse-checkout-set',
					repoPath: cwd,
					repoSubPaths
				});
				await applyWorkingTree(cwd, repoUrl, repoSubPaths);
				return 0;
			}

			if (gitArgs[0] === 'sparse-checkout' && gitArgs[1] === 'disable') {
				if (!cwd) return 1;
				const repoUrl = await readRepoOrigin(cwd);
				await writeCommandLog(logPath, { cmd: 'sparse-checkout-disable', repoPath: cwd });
				await applyWorkingTree(cwd, repoUrl, null);
				return 0;
			}

			if (gitArgs[0] === 'checkout') {
				await writeCommandLog(logPath, { cmd: 'checkout', repoPath: cwd ?? undefined });
				return 0;
			}

			if (gitArgs[0] === 'rev-parse' && gitArgs[1] === 'HEAD' && cwd) {
				return 0;
			}

			if (
				gitArgs[0] === 'rev-parse' &&
				gitArgs[1] === '--abbrev-ref' &&
				gitArgs[2] === 'HEAD' &&
				cwd
			) {
				return 0;
			}

			if (gitArgs[0] === 'rev-parse' && gitArgs[1] === '--is-inside-work-tree') {
				return cwd && (await pathExists(path.join(cwd, '.git'))) ? 0 : 128;
			}

			if (gitArgs[0] === 'remote' && gitArgs[1] === 'get-url' && gitArgs[2] === 'origin') {
				return cwd && (await pathExists(mockStatePaths(cwd).origin)) ? 0 : 1;
			}

			return 0;
		})();

		const stdout = (async () => {
			const cwd = options?.cwd;
			if (gitArgs[0] === 'rev-parse' && gitArgs[1] === 'HEAD' && cwd) {
				return await Bun.file(mockStatePaths(cwd).head)
					.text()
					.catch(() => 'fake-commit\n');
			}

			if (
				gitArgs[0] === 'rev-parse' &&
				gitArgs[1] === '--abbrev-ref' &&
				gitArgs[2] === 'HEAD' &&
				cwd
			) {
				const branch = await readRepoBranch(cwd);
				return branch.length > 0 ? branch : 'main';
			}

			if (gitArgs[0] === 'rev-parse' && gitArgs[1] === '--is-inside-work-tree') {
				return 'true\n';
			}

			if (gitArgs[0] === 'remote' && gitArgs[1] === 'get-url' && gitArgs[2] === 'origin' && cwd) {
				const origin = await readRepoOrigin(cwd);
				return origin.length > 0 ? origin : '';
			}

			return '';
		})();

		return {
			stderr: streamFromString(''),
			exited,
			stdout: new ReadableStream<Uint8Array>({
				async start(controller) {
					const value = await stdout;
					if (value.length > 0) controller.enqueue(new TextEncoder().encode(value));
					controller.close();
				}
			})
		} as unknown as ReturnType<typeof Bun.spawn>;
	}) as typeof Bun.spawn;

const createGitSpawnWithLsRemoteResponses = (
	logPath: string,
	responses: readonly { exitCode: number; stderr?: string; stdout?: string }[]
) => {
	const baseSpawn = createGitSpawnMock(logPath);
	let responseIndex = 0;

	return ((...spawnArgs: Parameters<typeof Bun.spawn>) => {
		const [command] = spawnArgs;
		const commandArgs = Array.isArray(command) ? command : [command];
		const gitArgs = commandArgs.slice(1);

		if (gitArgs[0] === 'ls-remote') {
			const response = responses[responseIndex] ??
				responses.at(-1) ?? {
					exitCode: 0,
					stdout: 'deadbeef\trefs/heads/main\n'
				};
			responseIndex += 1;

			return {
				stderr: streamFromString(response.stderr ?? ''),
				exited: Promise.resolve(response.exitCode),
				stdout: streamFromString(response.stdout ?? '')
			} as unknown as ReturnType<typeof Bun.spawn>;
		}

		return baseSpawn(...spawnArgs);
	}) as typeof Bun.spawn;
};

const readCommandLog = async (logPath: string) => {
	if (!(await pathExists(logPath))) return [];
	return (await Bun.file(logPath).text())
		.trim()
		.split('\n')
		.filter(Boolean)
		.map((line) => JSON.parse(line) as MockCommandLogEntry);
};

const materializeResource = async (
	resource: Awaited<ReturnType<typeof loadGitResource>>,
	destinationPath: string
) => {
	const vfsId = createVirtualFs();
	try {
		const result = await resource.materializeIntoVirtualFs({ destinationPath, vfsId });
		return { result, vfsId };
	} catch (cause) {
		disposeVirtualFs(vfsId);
		throw cause;
	}
};

const createGitTestDeps = (
	logPath: string,
	overrides?: Partial<GitResourceDeps>
): Partial<GitResourceDeps> => ({
	spawn: createGitSpawnMock(logPath),
	...(overrides ?? {})
});

const writeLiveClearLock = async (lockPath: string) => {
	await fs.mkdir(lockPath, { recursive: true });
	await Bun.write(
		path.join(lockPath, 'owner.json'),
		JSON.stringify(
			{
				pid: process.pid,
				token: 'live-clear-lock',
				label: 'resources.clear',
				startedAt: new Date().toISOString()
			},
			null,
			2
		)
	);
	await Bun.write(path.join(lockPath, 'heartbeat'), '');
};

describe('Git Resource', () => {
	let testDir: string;

	beforeEach(async () => {
		testDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-git-test-'));
	});

	afterEach(async () => {
		setFilesystemLockTestHookForTests();
		await fs.rm(testDir, { recursive: true, force: true });
	});

	it('throws error for invalid git URL', async () => {
		const args: BtcaGitResourceArgs = {
			type: 'git',
			name: 'invalid-url',
			url: 'not-a-valid-url',
			branch: 'main',
			repoSubPaths: [],
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: '',
			quiet: true
		};

		expect(loadGitResource(args)).rejects.toThrow('Git URL must be a valid HTTPS URL');
	});

	it('throws error for invalid branch name', async () => {
		const args: BtcaGitResourceArgs = {
			type: 'git',
			name: 'invalid-branch',
			url: 'https://github.com/test/repo',
			branch: 'invalid branch name!',
			repoSubPaths: [],
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: '',
			quiet: true
		};

		expect(loadGitResource(args)).rejects.toThrow('Branch name must contain only');
	});

	it('throws error for path traversal attempt', async () => {
		const args: BtcaGitResourceArgs = {
			type: 'git',
			name: 'path-traversal',
			url: 'https://github.com/test/repo',
			branch: 'main',
			repoSubPaths: ['../../../etc'],
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: '',
			quiet: true
		};

		expect(loadGitResource(args)).rejects.toThrow('path traversal');
	});

	it('imports named git repos into the VFS and excludes .git internals', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath);

		const resource = await loadGitResource(
			{
				type: 'git',
				name: 'docs',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: [],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true
			},
			gitDeps
		);

		const { vfsId } = await materializeResource(resource, '/docs');
		try {
			expect(await existsInVirtualFs('/docs/README.md', vfsId)).toBe(true);
			expect(await existsInVirtualFs('/docs/.git/config', vfsId)).toBe(false);
		} finally {
			disposeVirtualFs(vfsId);
		}
	});

	it('reclones a named mirror when the configured origin URL changes', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath);

		const first = await loadGitResource(
			{
				type: 'git',
				name: 'docs',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: [],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true
			},
			gitDeps
		);
		const firstMaterialized = await materializeResource(first, '/docs-a');
		disposeVirtualFs(firstMaterialized.vfsId);

		const second = await loadGitResource(
			{
				type: 'git',
				name: 'docs',
				url: 'https://example.com/repo-b.git',
				branch: 'main',
				repoSubPaths: [],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true
			},
			gitDeps
		);
		const { vfsId } = await materializeResource(second, '/docs-b');
		try {
			expect(await readVirtualFsFile('/docs-b/README.md', vfsId)).toContain('repo-b');
		} finally {
			disposeVirtualFs(vfsId);
		}

		const cloneCount = (await readCommandLog(logPath)).filter(
			(entry) => entry.cmd === 'clone'
		).length;
		expect(cloneCount).toBe(2);
	});

	it('reclones a named mirror when the existing directory is not a valid git working tree', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath);

		const mirrorPath = path.join(testDir, '.git-mirrors', 'docs', 'repo');
		await fs.mkdir(mirrorPath, { recursive: true });
		await fs.writeFile(path.join(mirrorPath, 'README.md'), 'broken mirror\n');

		const resource = await loadGitResource(
			{
				type: 'git',
				name: 'docs',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: [],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true
			},
			gitDeps
		);

		const { vfsId } = await materializeResource(resource, '/docs');
		try {
			expect(await readVirtualFsFile('/docs/README.md', vfsId)).toContain('repo-a');
		} finally {
			disposeVirtualFs(vfsId);
		}

		const cloneCount = (await readCommandLog(logPath)).filter(
			(entry) => entry.cmd === 'clone'
		).length;
		expect(cloneCount).toBe(1);
	});

	it('disables sparse-checkout when a named resource transitions to full-repo mode', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath);

		const sparseResource = await loadGitResource(
			{
				type: 'git',
				name: 'docs',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: ['docs'],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true
			},
			gitDeps
		);

		const sparseMaterialized = await materializeResource(sparseResource, '/docs-sparse');
		try {
			expect(await existsInVirtualFs('/docs-sparse/docs/guide.md', sparseMaterialized.vfsId)).toBe(
				true
			);
			expect(await existsInVirtualFs('/docs-sparse/README.md', sparseMaterialized.vfsId)).toBe(
				false
			);
		} finally {
			disposeVirtualFs(sparseMaterialized.vfsId);
		}

		const fullResource = await loadGitResource(
			{
				type: 'git',
				name: 'docs',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: [],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true
			},
			gitDeps
		);
		const fullMaterialized = await materializeResource(fullResource, '/docs-full');
		try {
			expect(await existsInVirtualFs('/docs-full/README.md', fullMaterialized.vfsId)).toBe(true);
			expect(await existsInVirtualFs('/docs-full/src/runtime.js', fullMaterialized.vfsId)).toBe(
				true
			);
		} finally {
			disposeVirtualFs(fullMaterialized.vfsId);
		}

		const commandLog = await readCommandLog(logPath);
		expect(commandLog.some((entry) => entry.cmd === 'sparse-checkout-disable')).toBe(true);
	});

	it('fails loudly when a repoSubPath is missing on first materialization', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath);

		const resource = await loadGitResource(
			{
				type: 'git',
				name: 'docs',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: ['missing'],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true
			},
			gitDeps
		);

		await expect(materializeResource(resource, '/docs')).rejects.toThrow(
			'Path not found: "missing"'
		);
	});

	it('fails loudly when a repoSubPath becomes missing after mirror reconcile', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath);

		const initial = await loadGitResource(
			{
				type: 'git',
				name: 'docs',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: ['docs'],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true
			},
			gitDeps
		);
		const initialMaterialized = await materializeResource(initial, '/docs-initial');
		disposeVirtualFs(initialMaterialized.vfsId);

		const updated = await loadGitResource(
			{
				type: 'git',
				name: 'docs',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: ['missing'],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true
			},
			gitDeps
		);

		await expect(materializeResource(updated, '/docs-updated')).rejects.toThrow(
			'Path not found: "missing"'
		);
	});

	it('re-waits on clear.lock before retrying named git materialization after post-acquire clear detection', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath);

		const resourceLockPath = path.join(testDir, '.resource-locks', 'git.docs.lock');
		const clearLockPath = path.join(testDir, '.resource-locks', 'clear.lock');
		let gitLockAcquireCount = 0;
		let injectedClearLock = false;
		let sawRetryAfterClear = false;

		setFilesystemLockTestHookForTests(async (event) => {
			if (
				event.phase === 'clear-aware-resource-lock-acquired' &&
				event.resourceLockPath === resourceLockPath
			) {
				gitLockAcquireCount += 1;
				if (!injectedClearLock) {
					injectedClearLock = true;
					await writeLiveClearLock(clearLockPath);
				}
			}

			if (
				event.phase === 'clear-aware-retry-after-clear' &&
				event.resourceLockPath === resourceLockPath &&
				injectedClearLock
			) {
				sawRetryAfterClear = true;
				await fs.rm(clearLockPath, { recursive: true, force: true });
			}
		});

		try {
			const resource = await loadGitResource(
				{
					type: 'git',
					name: 'docs',
					url: 'https://example.com/repo-a.git',
					branch: 'main',
					repoSubPaths: [],
					resourcesDirectoryPath: testDir,
					specialAgentInstructions: '',
					quiet: true
				},
				gitDeps
			);

			const { vfsId } = await materializeResource(resource, '/docs');
			try {
				expect(await readVirtualFsFile('/docs/README.md', vfsId)).toContain('repo-a');
			} finally {
				disposeVirtualFs(vfsId);
			}
		} finally {
			setFilesystemLockTestHookForTests();
		}

		expect(sawRetryAfterClear).toBe(true);
		expect(gitLockAcquireCount).toBe(2);
	});

	it('cleans up anonymous temp checkouts after repeated materialization', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath);

		const resource = await loadGitResource(
			{
				type: 'git',
				name: 'anonymous:https://example.com/repo-a.git',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: [],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true,
				ephemeral: true
			},
			gitDeps
		);

		const firstMaterialized = await materializeResource(resource, '/anonymous-first');
		disposeVirtualFs(firstMaterialized.vfsId);

		const [firstCloneTarget] = (await readCommandLog(logPath))
			.filter((entry) => entry.cmd === 'clone')
			.map((entry) => entry.repoPath as string);
		expect(firstCloneTarget).toBeDefined();
		expect(firstCloneTarget).toContain(
			`${getTmpCacheRoot(testDir)}${path.sep}btca-anon-git-anonymous%3Ahttps%3A%2F%2Fexample.com%2Frepo-a.git-`
		);
		expect(await pathExists(firstCloneTarget!)).toBe(true);

		const secondMaterialized = await materializeResource(resource, '/anonymous-second');
		disposeVirtualFs(secondMaterialized.vfsId);

		const cloneTargets = (await readCommandLog(logPath))
			.filter((entry) => entry.cmd === 'clone')
			.map((entry) => entry.repoPath as string);
		expect(cloneTargets).toHaveLength(2);
		expect(await pathExists(cloneTargets[0]!)).toBe(false);
		expect(await pathExists(cloneTargets[1]!)).toBe(true);

		await resource.cleanup?.();
		expect(await pathExists(cloneTargets[1]!)).toBe(false);
	});

	it('tries the next anonymous fallback branch when ls-remote reports a missing branch', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath, {
			spawn: createGitSpawnWithLsRemoteResponses(logPath, [
				{ exitCode: 2 },
				{ exitCode: 0, stdout: 'deadbeef\trefs/heads/master\n' }
			])
		});

		const resource = await loadGitResource(
			{
				type: 'git',
				name: 'anonymous:https://example.com/repo-a.git',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: [],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true,
				ephemeral: true
			},
			gitDeps
		);

		const { result, vfsId } = await materializeResource(resource, '/anonymous-master');
		try {
			expect(result.metadata.branch).toBe('master');
			expect(await readVirtualFsFile('/anonymous-master/README.md', vfsId)).toContain('repo-a');
		} finally {
			disposeVirtualFs(vfsId);
			await resource.cleanup?.();
		}

		const cloneEntries = (await readCommandLog(logPath)).filter((entry) => entry.cmd === 'clone');
		expect(cloneEntries).toHaveLength(1);
		expect(cloneEntries[0]?.branch).toBe('master');
	});

	it('fails immediately when anonymous branch probing hits a non-branch git error', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath, {
			spawn: createGitSpawnWithLsRemoteResponses(logPath, [
				{
					exitCode: 128,
					stderr: 'fatal: Authentication failed for https://example.com/repo-a.git'
				}
			])
		});

		const resource = await loadGitResource(
			{
				type: 'git',
				name: 'anonymous:https://example.com/repo-a.git',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: [],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true,
				ephemeral: true
			},
			gitDeps
		);

		await expect(materializeResource(resource, '/anonymous-auth-fail')).rejects.toThrow(
			'Authentication required or access denied'
		);
		await resource.cleanup?.();

		const cloneEntries = (await readCommandLog(logPath)).filter((entry) => entry.cmd === 'clone');
		expect(cloneEntries).toHaveLength(0);
	});

	it('cleans up anonymous temp checkouts if VFS import fails', async () => {
		const logPath = path.join(testDir, 'git-log.jsonl');
		const gitDeps = createGitTestDeps(logPath, {
			importDirectoryIntoVirtualFs: async (args) => {
				throw new Error(`forced import failure for ${args.sourcePath}`);
			}
		});

		const resource = await loadGitResource(
			{
				type: 'git',
				name: 'anonymous:https://example.com/repo-a.git',
				url: 'https://example.com/repo-a.git',
				branch: 'main',
				repoSubPaths: [],
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true,
				ephemeral: true
			},
			gitDeps
		);

		await expect(materializeResource(resource, '/anonymous-fail')).rejects.toThrow(
			'forced import failure'
		);

		const cloneTargets = (await readCommandLog(logPath))
			.filter((entry) => entry.cmd === 'clone')
			.map((entry) => entry.repoPath as string);
		expect(cloneTargets).toHaveLength(1);
		expect(await pathExists(cloneTargets[0]!)).toBe(false);
		await resource.cleanup?.();
	});
});
