import { promises as fs } from 'node:fs';
import path from 'node:path';

import { metricsInfo, withMetricsSpan } from '../../metrics/index.ts';
import { importDirectoryIntoVirtualFs } from '../../vfs/virtual-fs.ts';
import { CommonHints } from '../../errors.ts';
import { cleanupDirectory, directoryExists, pathExists } from '../fs-utils.ts';
import { ResourceError, resourceNameToKey, shouldIgnoreCommonImportedPath } from '../helpers.ts';
import { withClearAwareFilesystemLock } from '../lock.ts';
import {
	getClearLockPath,
	getGitLockPath,
	getGitMirrorRepoPath,
	getTmpCacheRoot
} from '../layout.ts';
import { GitResourceSchema } from '../schema.ts';
import type { BtcaGitFsResource, BtcaGitResourceArgs } from '../types.ts';

const ANONYMOUS_BRANCH_FALLBACKS = ['main', 'master', 'trunk', 'dev'];

type DisposableTempDir = {
	readonly path: string;
	readonly remove: () => Promise<void>;
};

type BunSpawnLike = typeof Bun.spawn;
type ImportDirectoryIntoVirtualFsLike = typeof importDirectoryIntoVirtualFs;

export type GitResourceDeps = {
	readonly spawn: BunSpawnLike;
	readonly importDirectoryIntoVirtualFs: ImportDirectoryIntoVirtualFsLike;
};

type ResolvedGitResourceArgs = {
	readonly name: string;
	readonly fsName: string;
	readonly resourceKey: string;
	readonly url: string;
	readonly branch: string;
	readonly repoSubPaths: readonly string[];
	readonly resourcesDirectoryPath: string;
	readonly specialAgentInstructions: string;
	readonly quiet: boolean;
	readonly ephemeral: boolean;
	readonly namedLocalPath: string;
	readonly clearLockPath: string;
	readonly resourceLockPath: string;
};

type FsPromisesWithDisposable = typeof fs & {
	readonly mkdtempDisposable?: (prefix: string) => Promise<{
		readonly path: string;
		readonly remove: () => Promise<void>;
	}>;
};

const createDisposableTempDir = async (prefix: string): Promise<DisposableTempDir> => {
	await fs.mkdir(path.dirname(prefix), { recursive: true });
	const fsWithDisposable = fs as FsPromisesWithDisposable;
	if (typeof fsWithDisposable.mkdtempDisposable === 'function') {
		const disposableDir = await fsWithDisposable.mkdtempDisposable(prefix);
		return {
			path: disposableDir.path,
			remove: async () => {
				try {
					await disposableDir.remove();
				} catch {
					await cleanupDirectory(disposableDir.path);
				}
			}
		};
	}

	const localPath = await fs.mkdtemp(prefix);
	return {
		path: localPath,
		remove: async () => {
			await cleanupDirectory(localPath);
		}
	};
};

const removeDisposableTempDir = async (tempDir: DisposableTempDir | null) => {
	if (!tempDir) return;
	await tempDir.remove();
};

const defaultGitResourceDeps: GitResourceDeps = {
	spawn: ((...spawnArgs: Parameters<typeof Bun.spawn>) =>
		Bun.spawn(...spawnArgs)) as typeof Bun.spawn,
	importDirectoryIntoVirtualFs
};

const resolveGitResourceDeps = (deps?: Partial<GitResourceDeps>): GitResourceDeps => ({
	spawn: deps?.spawn ?? defaultGitResourceDeps.spawn,
	importDirectoryIntoVirtualFs:
		deps?.importDirectoryIntoVirtualFs ?? defaultGitResourceDeps.importDirectoryIntoVirtualFs
});

const validateGitUrl = (url: string): { success: true } | { success: false; error: string } => {
	const result = GitResourceSchema.shape.url.safeParse(url);
	if (result.success) return { success: true };
	return { success: false, error: result.error.errors[0]?.message ?? 'Invalid git URL' };
};

const validateBranch = (branch: string): { success: true } | { success: false; error: string } => {
	const result = GitResourceSchema.shape.branch.safeParse(branch);
	if (result.success) return { success: true };
	return { success: false, error: result.error.errors[0]?.message ?? 'Invalid branch name' };
};

const validateSearchPath = (
	searchPath: string
): { success: true } | { success: false; error: string } => {
	const result = GitResourceSchema.shape.searchPath.safeParse(searchPath);
	if (result.success) return { success: true };
	return { success: false, error: result.error.errors[0]?.message ?? 'Invalid search path' };
};

const resolveGitResourceArgs = (config: BtcaGitResourceArgs): ResolvedGitResourceArgs => {
	const urlValidation = validateGitUrl(config.url);
	if (!urlValidation.success) {
		throw new ResourceError({
			message: urlValidation.error,
			hint: 'URLs must be valid HTTPS URLs. Example: https://github.com/user/repo',
			cause: new Error('URL validation failed')
		});
	}

	const branchValidation = validateBranch(config.branch);
	if (!branchValidation.success) {
		throw new ResourceError({
			message: branchValidation.error,
			hint: 'Branch names can only contain letters, numbers, hyphens, underscores, dots, and forward slashes.',
			cause: new Error('Branch validation failed')
		});
	}

	for (const repoSubPath of config.repoSubPaths) {
		const pathValidation = validateSearchPath(repoSubPath);
		if (!pathValidation.success) {
			throw new ResourceError({
				message: pathValidation.error,
				hint: 'Search paths cannot contain ".." (path traversal) and must use only safe characters.',
				cause: new Error('Path validation failed')
			});
		}
	}

	const resourceKey = config.localDirectoryKey ?? resourceNameToKey(config.name);
	return {
		name: config.name,
		fsName: resourceNameToKey(config.name),
		resourceKey,
		url: config.url,
		branch: config.branch,
		repoSubPaths: [...config.repoSubPaths],
		resourcesDirectoryPath: config.resourcesDirectoryPath,
		specialAgentInstructions: config.specialAgentInstructions,
		quiet: config.quiet,
		ephemeral: config.ephemeral ?? false,
		namedLocalPath: getGitMirrorRepoPath(config.resourcesDirectoryPath, resourceKey),
		clearLockPath: getClearLockPath(config.resourcesDirectoryPath),
		resourceLockPath: getGitLockPath(config.resourcesDirectoryPath, resourceKey)
	};
};

const readGitStdout = async (
	args: string[],
	resourcePath: string,
	deps: Pick<GitResourceDeps, 'spawn'>
) => {
	try {
		const proc = deps.spawn(['git', ...args], {
			cwd: resourcePath,
			stdout: 'pipe',
			stderr: 'pipe'
		});
		const stdout = await new Response(proc.stdout).text();
		const exitCode = await proc.exited;
		if (exitCode !== 0) return null;
		return stdout.trim();
	} catch {
		return null;
	}
};

const getGitHeadHash = async (resourcePath: string, deps: Pick<GitResourceDeps, 'spawn'>) => {
	const stdout = await readGitStdout(['rev-parse', 'HEAD'], resourcePath, deps);
	return stdout && stdout.length > 0 ? stdout : undefined;
};

const isGitWorkingTree = async (resourcePath: string, deps: Pick<GitResourceDeps, 'spawn'>) => {
	const stdout = await readGitStdout(['rev-parse', '--is-inside-work-tree'], resourcePath, deps);
	return stdout === 'true';
};

const getGitOriginUrl = async (resourcePath: string, deps: Pick<GitResourceDeps, 'spawn'>) => {
	const stdout = await readGitStdout(['remote', 'get-url', 'origin'], resourcePath, deps);
	return stdout && stdout.length > 0 ? stdout : undefined;
};

/**
 * Git error patterns and their user-friendly messages.
 */
const GitErrorPatterns = {
	// Branch not found errors
	BRANCH_NOT_FOUND: [
		/couldn't find remote ref/i,
		/Remote branch .* not found/i,
		/fatal: invalid refspec/i,
		/error: pathspec .* did not match any/i
	],
	// Repository not found
	REPO_NOT_FOUND: [
		/Repository not found/i,
		/remote: Repository not found/i,
		/fatal: repository .* not found/i,
		/ERROR: Repository not found/i
	],
	// Authentication/Permission errors
	AUTH_REQUIRED: [
		/Authentication failed/i,
		/could not read Username/i,
		/Permission denied/i,
		/fatal: Authentication failed/i,
		/remote: HTTP Basic: Access denied/i,
		/The requested URL returned error: 403/i
	],
	// Network errors
	NETWORK_ERROR: [
		/Could not resolve host/i,
		/Connection refused/i,
		/Network is unreachable/i,
		/Unable to access/i,
		/Failed to connect/i,
		/Connection timed out/i,
		/SSL certificate problem/i
	],
	// Rate limiting
	RATE_LIMITED: [/rate limit exceeded/i, /too many requests/i, /API rate limit/i]
} as const;

type GitErrorType = keyof typeof GitErrorPatterns;

/**
 * Detect the type of git error from stderr output.
 */
const detectGitErrorType = (stderr: string): GitErrorType | null => {
	for (const [errorType, patterns] of Object.entries(GitErrorPatterns)) {
		for (const pattern of patterns) {
			if (pattern.test(stderr)) {
				return errorType as GitErrorType;
			}
		}
	}
	return null;
};

/**
 * Get a user-friendly error message and hint based on git error type.
 */
const getGitErrorDetails = (
	errorType: GitErrorType | null,
	context: { operation: string; branch?: string; url?: string }
): { message: string; hint: string } => {
	switch (errorType) {
		case 'BRANCH_NOT_FOUND':
			return {
				message: context.branch
					? `Branch "${context.branch}" not found in the repository`
					: 'The specified branch was not found',
				hint: `${CommonHints.CHECK_BRANCH} You can check available branches at ${context.url ?? 'the repository URL'}.`
			};

		case 'REPO_NOT_FOUND':
			return {
				message: 'Repository not found',
				hint: `${CommonHints.CHECK_URL} If this is a private repository, ${CommonHints.CHECK_PERMISSIONS.toLowerCase()}`
			};

		case 'AUTH_REQUIRED':
			return {
				message: 'Authentication required or access denied',
				hint: CommonHints.CHECK_PERMISSIONS
			};

		case 'NETWORK_ERROR':
			return {
				message: `Network error during git ${context.operation}`,
				hint: CommonHints.CHECK_NETWORK
			};

		case 'RATE_LIMITED':
			return {
				message: 'Rate limit exceeded',
				hint: 'Wait a few minutes before trying again, or authenticate to increase your rate limit.'
			};

		default:
			return {
				message: `git ${context.operation} failed`,
				hint: `${CommonHints.CLEAR_CACHE} If the problem persists, verify your repository configuration.`
			};
	}
};

interface GitRunResult {
	exitCode: number;
	stderr: string;
}

const runGitChecked = async (
	args: string[],
	options: { cwd?: string; quiet: boolean },
	buildError: (result: GitRunResult) => ResourceError,
	deps: Pick<GitResourceDeps, 'spawn'>
) => {
	const runResult = await runGit(args, options, deps);
	if (runResult.exitCode !== 0) {
		throw buildError(runResult);
	}
	return runResult;
};

const runGit = async (
	args: string[],
	options: { cwd?: string; quiet: boolean },
	deps: Pick<GitResourceDeps, 'spawn'>
): Promise<GitRunResult> => {
	// Always capture stderr for error detection, but stdout can be ignored
	const proc = deps.spawn(['git', ...args], {
		cwd: options.cwd,
		stdout: options.quiet ? 'ignore' : 'inherit',
		stderr: 'pipe'
	});

	const stderrChunks: Uint8Array[] = [];
	const reader = proc.stderr.getReader();

	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			if (value) stderrChunks.push(value);
		}
	} finally {
		reader.releaseLock();
	}

	const exitCode = await proc.exited;
	const totalLength = stderrChunks.reduce((sum, chunk) => sum + chunk.length, 0);
	const combined = new Uint8Array(totalLength);
	let offset = 0;
	for (const chunk of stderrChunks) {
		combined.set(chunk, offset);
		offset += chunk.length;
	}
	const stderr = new TextDecoder().decode(combined);

	// Log stderr to console if not quiet and there's content
	if (!options.quiet && stderr.trim()) {
		console.error(stderr);
	}

	return { exitCode, stderr };
};

const gitClone = async (
	args: {
		repoUrl: string;
		repoBranch: string;
		repoSubPaths: readonly string[];
		localAbsolutePath: string;
		quiet: boolean;
	},
	deps: Pick<GitResourceDeps, 'spawn'>
) => {
	const needsSparseCheckout = args.repoSubPaths.length > 0;
	const cloneArgs = needsSparseCheckout
		? [
				'clone',
				'--filter=blob:none',
				'--no-checkout',
				'--sparse',
				'-b',
				args.repoBranch,
				args.repoUrl,
				args.localAbsolutePath
			]
		: ['clone', '--depth', '1', '-b', args.repoBranch, args.repoUrl, args.localAbsolutePath];

	await runGitChecked(
		cloneArgs,
		{ quiet: args.quiet },
		(cloneResult) => {
			const errorType = detectGitErrorType(cloneResult.stderr);
			const { message, hint } = getGitErrorDetails(errorType, {
				operation: 'clone',
				branch: args.repoBranch,
				url: args.repoUrl
			});

			return new ResourceError({
				message,
				hint,
				cause: new Error(
					`git clone failed with exit code ${cloneResult.exitCode}: ${cloneResult.stderr}`
				)
			});
		},
		deps
	);

	if (needsSparseCheckout) {
		await runGitChecked(
			['sparse-checkout', 'set', ...args.repoSubPaths],
			{ cwd: args.localAbsolutePath, quiet: args.quiet },
			(sparseResult) =>
				new ResourceError({
					message: `Failed to set sparse-checkout path(s): "${args.repoSubPaths.join(', ')}"`,
					hint: 'Verify the search paths exist in the repository. Check the repository structure to find the correct path.',
					cause: new Error(
						`git sparse-checkout failed with exit code ${sparseResult.exitCode}: ${sparseResult.stderr}`
					)
				}),
			deps
		);

		await runGitChecked(
			['checkout'],
			{ cwd: args.localAbsolutePath, quiet: args.quiet },
			(checkout) =>
				new ResourceError({
					message: 'Failed to checkout repository',
					hint: CommonHints.CLEAR_CACHE,
					cause: new Error(
						`git checkout failed with exit code ${checkout.exitCode}: ${checkout.stderr}`
					)
				}),
			deps
		);
	}
};

const probeRemoteBranch = async (
	args: {
		repoUrl: string;
		repoBranch: string;
	},
	deps: Pick<GitResourceDeps, 'spawn'>
): Promise<'exists' | 'missing'> => {
	const probeResult = await runGit(
		['ls-remote', '--exit-code', '--heads', args.repoUrl, `refs/heads/${args.repoBranch}`],
		{ quiet: true },
		deps
	);

	if (probeResult.exitCode === 0) return 'exists';
	if (probeResult.exitCode === 2) return 'missing';

	const errorType = detectGitErrorType(probeResult.stderr);
	const { message, hint } = getGitErrorDetails(errorType, {
		operation: 'probe remote branch',
		branch: args.repoBranch,
		url: args.repoUrl
	});
	throw new ResourceError({
		message,
		hint,
		cause: new Error(
			`git ls-remote failed with exit code ${probeResult.exitCode}: ${probeResult.stderr}`
		)
	});
};

const isSparseCheckoutEnabled = async (localAbsolutePath: string) =>
	await pathExists(path.join(localAbsolutePath, '.git', 'info', 'sparse-checkout'));

const applyMirrorCheckoutMode = async (
	args: {
		localAbsolutePath: string;
		repoSubPaths: readonly string[];
		quiet: boolean;
	},
	deps: Pick<GitResourceDeps, 'spawn'>
) => {
	if (args.repoSubPaths.length > 0) {
		await runGitChecked(
			['sparse-checkout', 'set', ...args.repoSubPaths],
			{ cwd: args.localAbsolutePath, quiet: args.quiet },
			(sparseResult) =>
				new ResourceError({
					message: `Failed to set sparse-checkout path(s): "${args.repoSubPaths.join(', ')}"`,
					hint: 'Verify the search paths exist in the repository. Check the repository structure to find the correct path.',
					cause: new Error(
						`git sparse-checkout failed with exit code ${sparseResult.exitCode}: ${sparseResult.stderr}`
					)
				}),
			deps
		);

		await runGitChecked(
			['checkout'],
			{ cwd: args.localAbsolutePath, quiet: args.quiet },
			(checkoutResult) =>
				new ResourceError({
					message: 'Failed to checkout repository',
					hint: CommonHints.CLEAR_CACHE,
					cause: new Error(
						`git checkout failed with exit code ${checkoutResult.exitCode}: ${checkoutResult.stderr}`
					)
				}),
			deps
		);
		return;
	}

	if (!(await isSparseCheckoutEnabled(args.localAbsolutePath))) return;

	await runGitChecked(
		['sparse-checkout', 'disable'],
		{ cwd: args.localAbsolutePath, quiet: args.quiet },
		(disableResult) =>
			new ResourceError({
				message: 'Failed to disable sparse-checkout for full repository mode',
				hint: `${CommonHints.CLEAR_CACHE} This will re-clone the repository from scratch.`,
				cause: new Error(
					`git sparse-checkout disable failed with exit code ${disableResult.exitCode}: ${disableResult.stderr}`
				)
			}),
		deps
	);
};

const gitUpdate = async (
	args: {
		localAbsolutePath: string;
		branch: string;
		repoSubPaths: readonly string[];
		quiet: boolean;
	},
	deps: Pick<GitResourceDeps, 'spawn'>
) => {
	await runGitChecked(
		['fetch', '--depth', '1', 'origin', args.branch],
		{ cwd: args.localAbsolutePath, quiet: args.quiet },
		(fetchResult) => {
			const errorType = detectGitErrorType(fetchResult.stderr);
			const { message, hint } = getGitErrorDetails(errorType, {
				operation: 'fetch',
				branch: args.branch
			});

			return new ResourceError({
				message,
				hint,
				cause: new Error(
					`git fetch failed with exit code ${fetchResult.exitCode}: ${fetchResult.stderr}`
				)
			});
		},
		deps
	);

	await runGitChecked(
		['reset', '--hard', `origin/${args.branch}`],
		{ cwd: args.localAbsolutePath, quiet: args.quiet },
		(resetResult) =>
			new ResourceError({
				message: 'Failed to update local repository',
				hint: `${CommonHints.CLEAR_CACHE} This will re-clone the repository from scratch.`,
				cause: new Error(
					`git reset failed with exit code ${resetResult.exitCode}: ${resetResult.stderr}`
				)
			}),
		deps
	);

	await applyMirrorCheckoutMode(args, deps);
};

/**
 * Detect common mistakes in searchPath and provide helpful hints.
 */
const getSearchPathHint = (searchPath: string, repoPath: string): string => {
	// Pattern: GitHub URL structure like "tree/main/path" or "blob/dev/path"
	const gitHubTreeMatch = searchPath.match(/^(tree|blob)\/([^/]+)\/(.+)$/);
	if (gitHubTreeMatch) {
		const [, , branch, actualPath] = gitHubTreeMatch;
		return `It looks like you included the GitHub URL structure. Remove '${gitHubTreeMatch[1]}/${branch}/' prefix and use: "${actualPath}"`;
	}

	// Pattern: full URL included
	if (searchPath.startsWith('http://') || searchPath.startsWith('https://')) {
		return 'searchPath should be a relative path within the repo, not a URL. Extract just the directory path after the branch name.';
	}

	// Pattern: starts with domain
	if (searchPath.includes('github.com') || searchPath.includes('gitlab.com')) {
		return "searchPath should be a relative path within the repo, not a URL. Use just the directory path, e.g., 'src/docs'";
	}

	// Default hint with helpful command
	return `Verify the path exists in the repository. To see available directories, run:\n  ls ${repoPath}`;
};

const ensureResolvedRepoSubPathsExist = async (
	localPath: string,
	repoSubPaths: readonly string[],
	resourceName: string
): Promise<void> => {
	for (const repoSubPath of repoSubPaths) {
		const subPath = path.join(localPath, repoSubPath);
		const exists = await directoryExists(subPath);
		if (!exists) {
			const hint = getSearchPathHint(repoSubPath, localPath);
			throw new ResourceError({
				message: `Invalid searchPath for resource "${resourceName}"\n\nPath not found: "${repoSubPath}"\nRepository: ${localPath}`,
				hint,
				cause: new Error(`Missing search path: ${repoSubPath}`)
			});
		}
	}
};

const ensureMirrorDirectories = async (config: ResolvedGitResourceArgs) => {
	const basePath = path.dirname(config.namedLocalPath);
	try {
		await fs.mkdir(basePath, { recursive: true });
	} catch (cause) {
		throw new ResourceError({
			message: 'Failed to create resources directory',
			hint: 'Check that you have write permissions to the btca data directory.',
			cause
		});
	}
};

const mirrorRepoExists = async (config: ResolvedGitResourceArgs) =>
	await directoryExists(config.namedLocalPath);

const readMirrorOriginUrl = async (
	config: ResolvedGitResourceArgs,
	deps: Pick<GitResourceDeps, 'spawn'>
) => await getGitOriginUrl(config.namedLocalPath, deps);

const cloneNamedMirror = async (
	config: ResolvedGitResourceArgs,
	deps: Pick<GitResourceDeps, 'spawn'>
) => {
	await gitClone(
		{
			repoUrl: config.url,
			repoBranch: config.branch,
			repoSubPaths: config.repoSubPaths,
			localAbsolutePath: config.namedLocalPath,
			quiet: config.quiet
		},
		deps
	);
};

const updateNamedMirror = async (
	config: ResolvedGitResourceArgs,
	deps: Pick<GitResourceDeps, 'spawn'>
) => {
	await gitUpdate(
		{
			localAbsolutePath: config.namedLocalPath,
			branch: config.branch,
			repoSubPaths: config.repoSubPaths,
			quiet: config.quiet
		},
		deps
	);
};

const readHeadCommit = async (localPath: string, deps: Pick<GitResourceDeps, 'spawn'>) =>
	await getGitHeadHash(localPath, deps);

const reconcileNamedMirror = async (
	config: ResolvedGitResourceArgs,
	deps: Pick<GitResourceDeps, 'spawn'>
): Promise<{ localPath: string }> => {
	await ensureMirrorDirectories(config);
	return withMetricsSpan(
		'resource.git.ensure',
		async () => {
			if (await mirrorRepoExists(config)) {
				const validGitWorkingTree = await isGitWorkingTree(config.namedLocalPath, deps);
				if (!validGitWorkingTree) {
					await cleanupDirectory(config.namedLocalPath);
				} else {
					const originUrl = await readMirrorOriginUrl(config, deps);
					if (originUrl !== config.url) {
						await cleanupDirectory(config.namedLocalPath);
					}
				}
			}

			if (await mirrorRepoExists(config)) {
				metricsInfo('resource.git.update', {
					name: config.name,
					branch: config.branch,
					repoSubPaths: config.repoSubPaths
				});
				await updateNamedMirror(config, deps);
				if (config.repoSubPaths.length > 0) {
					await ensureResolvedRepoSubPathsExist(
						config.namedLocalPath,
						config.repoSubPaths,
						config.name
					);
				}
				return { localPath: config.namedLocalPath };
			}

			metricsInfo('resource.git.clone', {
				name: config.name,
				branch: config.branch,
				repoSubPaths: config.repoSubPaths
			});

			await cloneNamedMirror(config, deps);
			if (config.repoSubPaths.length > 0) {
				await ensureResolvedRepoSubPathsExist(
					config.namedLocalPath,
					config.repoSubPaths,
					config.name
				);
			}

			return { localPath: config.namedLocalPath };
		},
		{ resource: config.name }
	);
};

const materializeAnonymousGitResource = async (
	config: ResolvedGitResourceArgs,
	deps: Pick<GitResourceDeps, 'spawn'>
): Promise<{ localPath: string; branch: string; tempDir: DisposableTempDir }> => {
	const tempDir = await createDisposableTempDir(
		path.join(
			getTmpCacheRoot(config.resourcesDirectoryPath),
			`btca-anon-git-${config.resourceKey}-`
		)
	);
	let lastBranchError: unknown;

	for (const branch of ANONYMOUS_BRANCH_FALLBACKS) {
		const branchStatus = await probeRemoteBranch(
			{
				repoUrl: config.url,
				repoBranch: branch
			},
			deps
		);
		if (branchStatus === 'missing') continue;

		try {
			await fs.mkdir(tempDir.path, { recursive: true });
			await gitClone(
				{
					repoUrl: config.url,
					repoBranch: branch,
					repoSubPaths: config.repoSubPaths,
					localAbsolutePath: tempDir.path,
					quiet: config.quiet
				},
				deps
			);
			if (config.repoSubPaths.length > 0) {
				await ensureResolvedRepoSubPathsExist(tempDir.path, config.repoSubPaths, config.name);
			}
			return { localPath: tempDir.path, branch, tempDir };
		} catch (error) {
			lastBranchError = error;
			await cleanupDirectory(tempDir.path);
			throw error;
		}
	}

	await removeDisposableTempDir(tempDir);

	throw new ResourceError({
		message: `Could not find this repository on a common branch. Tried ${ANONYMOUS_BRANCH_FALLBACKS.join(
			', '
		)}.`,
		hint: 'If the repo uses a different branch, add it as a named resource and use that name. See https://docs.btca.dev/guides/configuration.',
		cause: lastBranchError
	});
};

export const loadGitResource = async (
	config: BtcaGitResourceArgs,
	deps?: Partial<GitResourceDeps>
): Promise<BtcaGitFsResource> => {
	const resolved = resolveGitResourceArgs(config);
	const gitDeps = resolveGitResourceDeps(deps);

	let anonymousTempDir: DisposableTempDir | null = null;
	let latestAnonymousMaterializationId = 0;
	const cleanup = resolved.ephemeral
		? async () => {
				latestAnonymousMaterializationId += 1;
				const currentTempDir = anonymousTempDir;
				anonymousTempDir = null;
				await removeDisposableTempDir(currentTempDir);
			}
		: undefined;

	return {
		_tag: 'fs-based',
		name: resolved.name,
		fsName: resolved.fsName,
		type: 'git',
		repoSubPaths: resolved.repoSubPaths,
		specialAgentInstructions: resolved.specialAgentInstructions,
		materializeIntoVirtualFs: async ({ destinationPath, vfsId }) => {
			const materializeAndImport = async () => {
				const materializationId = resolved.ephemeral ? latestAnonymousMaterializationId + 1 : 0;
				if (resolved.ephemeral) latestAnonymousMaterializationId = materializationId;

				if (resolved.ephemeral) {
					const materialized = await materializeAnonymousGitResource(resolved, gitDeps);
					const commit = await readHeadCommit(materialized.localPath, gitDeps);
					try {
						await gitDeps.importDirectoryIntoVirtualFs({
							sourcePath: materialized.localPath,
							destinationPath,
							vfsId,
							ignore: shouldIgnoreCommonImportedPath
						});

						if (materializationId === latestAnonymousMaterializationId) {
							const previousTempDir = anonymousTempDir;
							anonymousTempDir = materialized.tempDir;
							await removeDisposableTempDir(previousTempDir);
						} else {
							await removeDisposableTempDir(materialized.tempDir);
						}

						return {
							metadata: {
								url: resolved.url,
								branch: materialized.branch,
								...(commit ? { commit } : {})
							}
						};
					} catch (cause) {
						await removeDisposableTempDir(materialized.tempDir);
						throw cause;
					}
				}

				const materialized = await reconcileNamedMirror(resolved, gitDeps);
				const commit = await readHeadCommit(materialized.localPath, gitDeps);

				await gitDeps.importDirectoryIntoVirtualFs({
					sourcePath: materialized.localPath,
					destinationPath,
					vfsId,
					ignore: shouldIgnoreCommonImportedPath
				});

				return {
					metadata: {
						url: resolved.url,
						branch: resolved.branch,
						...(commit ? { commit } : {})
					}
				};
			};

			return withClearAwareFilesystemLock(
				{
					clearLockPath: resolved.clearLockPath,
					clearLockWaitLabel: `wait-clear-before-git.${resolved.resourceKey}`,
					clearLockInspectLabel: `inspect-clear-before-git.${resolved.resourceKey}`,
					resourceLockPath: resolved.resourceLockPath,
					resourceLockLabel: `git.${resolved.resourceKey}`,
					quiet: resolved.quiet
				},
				materializeAndImport
			);
		},
		...(cleanup ? { cleanup } : {})
	};
};
