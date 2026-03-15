import { promises as fs } from 'node:fs';
import path from 'node:path';

import { importDirectoryIntoVirtualFs } from '../../vfs/virtual-fs.ts';
import { CommonHints } from '../../errors.ts';
import { cleanupDirectory, directoryExists } from '../fs-utils.ts';
import { ResourceError, resourceNameToKey, shouldIgnoreCommonImportedPath } from '../helpers.ts';
import { withClearAwareFilesystemLock, withFilesystemLock } from '../lock.ts';
import { TMP_DIR, getClearLockPath, getNpmLockPath } from '../layout.ts';
import type { BtcaNpmFsResource, BtcaNpmResourceArgs } from '../types.ts';

const NPM_INSTALL_STAGING_DIR = '.btca-install';
const NPM_CACHE_META_FILE = '.btca-npm-meta.json';
const NPM_CONTENT_FILE = 'npm-package.md';
const NPM_PAGE_FILE = 'npm-package-page.html';
const NPM_REGISTRY_HOST = 'https://registry.npmjs.org';

type NpmCacheMeta = {
	packageName: string;
	requestedVersion?: string;
	resolvedVersion: string;
	packageUrl: string;
	pageUrl: string;
	fetchedAt: string;
};

type NpmPackument = {
	readonly 'dist-tags'?: Record<string, string | undefined>;
	readonly versions?: Record<string, NpmPackageVersion | undefined>;
	readonly readme?: string;
};

type NpmPackageVersion = {
	readonly name?: string;
	readonly version?: string;
	readonly description?: string;
	readonly homepage?: string;
	readonly repository?: { url?: string } | string;
	readonly license?: string;
	readonly keywords?: readonly string[];
	readonly dependencies?: Record<string, string | undefined>;
	readonly peerDependencies?: Record<string, string | undefined>;
};

type BunSpawnLike = typeof Bun.spawn;

export type NpmResourceDeps = {
	readonly spawn: BunSpawnLike;
};

type ResolvedNpmResourceArgs = {
	readonly name: string;
	readonly fsName: string;
	readonly cacheKey: string;
	readonly package: string;
	readonly requestedVersion?: string;
	readonly resourcesDirectoryPath: string;
	readonly specialAgentInstructions: string;
	readonly quiet: boolean;
	readonly ephemeral: boolean;
	readonly localPath: string;
	readonly clearLockPath: string;
	readonly resourceLockPath: string;
};

const sanitizeNpmCitationSegment = (value: string) =>
	value
		.trim()
		.replace(/\//g, '__')
		.replace(/[^a-zA-Z0-9._@+-]+/g, '-')
		.replace(/-+/g, '-')
		.replace(/^-|-$/g, '');

const getResolvedNpmFsName = (config: BtcaNpmResourceArgs) => {
	if (!config.ephemeral || !config.name.startsWith('anonymous:npm:')) {
		return resourceNameToKey(config.name);
	}

	const packageSegment = sanitizeNpmCitationSegment(config.package);
	const versionSegment = sanitizeNpmCitationSegment(config.version ?? 'latest');
	const cacheSuffix = config.localDirectoryKey ?? resourceNameToKey(config.name);
	return `npm:${packageSegment}@${versionSegment}--${cacheSuffix}`;
};

const resolveNpmResourceArgs = (config: BtcaNpmResourceArgs): ResolvedNpmResourceArgs => {
	const cacheKey = config.localDirectoryKey ?? resourceNameToKey(config.name);
	const localBasePath =
		config.ephemeral === true
			? path.join(config.resourcesDirectoryPath, TMP_DIR)
			: config.resourcesDirectoryPath;

	return {
		name: config.name,
		fsName: getResolvedNpmFsName(config),
		cacheKey,
		package: config.package,
		...(config.version ? { requestedVersion: config.version } : {}),
		resourcesDirectoryPath: config.resourcesDirectoryPath,
		specialAgentInstructions: config.specialAgentInstructions,
		quiet: config.quiet,
		ephemeral: config.ephemeral ?? false,
		localPath: path.join(localBasePath, cacheKey),
		clearLockPath: getClearLockPath(config.resourcesDirectoryPath),
		resourceLockPath: getNpmLockPath(config.resourcesDirectoryPath, cacheKey)
	};
};

const encodePackagePath = (packageName: string) =>
	packageName.split('/').map(encodeURIComponent).join('/');

const formatRepositoryUrl = (repository: NpmPackageVersion['repository']) => {
	if (!repository) return undefined;
	if (typeof repository === 'string') return repository;
	return repository.url;
};

const resolveRequestedVersion = (packument: NpmPackument, requestedVersion?: string) => {
	const versions = packument.versions ?? {};
	const distTags = packument['dist-tags'] ?? {};
	const requested = requestedVersion?.trim();

	if (!requested) {
		const latest = distTags.latest;
		if (latest && versions[latest]) return latest;
		return null;
	}

	if (versions[requested]) return requested;
	const tagged = distTags[requested];
	if (tagged && versions[tagged]) return tagged;
	return null;
};

const fetchJson = async <T>(url: string, resourceName: string): Promise<T> => {
	let response: Response;
	try {
		response = await fetch(url, {
			headers: {
				accept: 'application/json'
			}
		});
	} catch (cause) {
		throw new ResourceError({
			message: `Failed to fetch npm metadata for "${resourceName}"`,
			hint: CommonHints.CHECK_NETWORK,
			cause
		});
	}

	if (!response.ok) {
		throw new ResourceError({
			message: `Failed to fetch npm metadata for "${resourceName}" (${response.status})`,
			hint:
				response.status === 404 ? 'Check that the npm package exists.' : CommonHints.CHECK_NETWORK,
			cause: new Error(`Unexpected status ${response.status}`)
		});
	}

	try {
		return (await response.json()) as T;
	} catch (cause) {
		throw new ResourceError({
			message: `Failed to parse npm metadata for "${resourceName}"`,
			hint: 'Try again. If the issue persists, the npm registry may be returning malformed data.',
			cause
		});
	}
};

const fetchText = async (url: string, resourceName: string) => {
	const fallbackContent = (reason: string) =>
		`<!-- npm package page unavailable for "${resourceName}" (${reason}) -->`;

	let response: Response;
	try {
		response = await fetch(url);
	} catch {
		return fallbackContent('request failed');
	}

	if (!response.ok) {
		return fallbackContent(`status ${response.status}`);
	}

	try {
		return await response.text();
	} catch {
		return fallbackContent('response read failed');
	}
};

const readProcessOutput = async (stream: ReadableStream<Uint8Array> | null) => {
	if (!stream) return '';
	const reader = stream.getReader();
	const chunks: Uint8Array[] = [];

	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			if (value) chunks.push(value);
		}
	} finally {
		reader.releaseLock();
	}

	const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
	const merged = new Uint8Array(totalLength);
	let offset = 0;
	for (const chunk of chunks) {
		merged.set(chunk, offset);
		offset += chunk.length;
	}

	return new TextDecoder().decode(merged);
};

const defaultNpmResourceDeps: NpmResourceDeps = {
	spawn: ((...spawnArgs: Parameters<typeof Bun.spawn>) =>
		Bun.spawn(...spawnArgs)) as typeof Bun.spawn
};

const resolveNpmResourceDeps = (deps?: Partial<NpmResourceDeps>): NpmResourceDeps => ({
	spawn: deps?.spawn ?? defaultNpmResourceDeps.spawn
});

const runBunInstall = async (
	args: {
		installDirectory: string;
		packageName: string;
		resolvedVersion: string;
	},
	deps: NpmResourceDeps
) => {
	const packageSpec = `${args.packageName}@${args.resolvedVersion}`;
	const command = ['bun', 'add', '--exact', '--ignore-scripts', packageSpec];
	const process = deps.spawn(command, {
		cwd: args.installDirectory,
		stdout: 'pipe',
		stderr: 'pipe'
	});

	const [stdout, stderr, exitCode] = await Promise.all([
		readProcessOutput(process.stdout),
		readProcessOutput(process.stderr),
		process.exited
	]);

	if (exitCode !== 0) {
		const details = [stderr.trim(), stdout.trim()].filter(Boolean).join('\n');
		throw new ResourceError({
			message: `Failed to install npm package "${packageSpec}"`,
			hint: 'Check that the package/version exists and your network can reach npm. Try "btca clear" and run again.',
			cause: new Error(
				details.length > 0
					? `bun add exited ${exitCode}: ${details}`
					: `bun add exited ${exitCode} with no output`
			)
		});
	}
};

const formatResourceOverview = (args: {
	packageName: string;
	resolvedVersion: string;
	requestedVersion?: string;
	packageUrl: string;
	pageUrl: string;
	versionData: NpmPackageVersion;
}) => {
	const dependencies = Object.entries(args.versionData.dependencies ?? {})
		.slice(0, 100)
		.map(([name, version]) => `- ${name}: ${version ?? 'unknown'}`)
		.join('\n');
	const peerDependencies = Object.entries(args.versionData.peerDependencies ?? {})
		.slice(0, 100)
		.map(([name, version]) => `- ${name}: ${version ?? 'unknown'}`)
		.join('\n');
	const repositoryUrl = formatRepositoryUrl(args.versionData.repository);

	return [
		`# npm package: ${args.packageName}`,
		'',
		`- Package URL: ${args.packageUrl}`,
		`- npm page: ${args.pageUrl}`,
		`- Version: ${args.resolvedVersion}`,
		args.requestedVersion
			? `- Requested version/tag: ${args.requestedVersion}`
			: '- Requested version/tag: latest',
		args.versionData.description ? `- Description: ${args.versionData.description}` : '',
		args.versionData.homepage ? `- Homepage: ${args.versionData.homepage}` : '',
		repositoryUrl ? `- Repository: ${repositoryUrl}` : '',
		args.versionData.license ? `- License: ${args.versionData.license}` : '',
		args.versionData.keywords?.length ? `- Keywords: ${args.versionData.keywords.join(', ')}` : '',
		'',
		'## Dependencies',
		dependencies || 'No dependencies listed.',
		'',
		'## Peer Dependencies',
		peerDependencies || 'No peer dependencies listed.'
	]
		.filter(Boolean)
		.join('\n');
};

const readCacheMeta = async (localPath: string): Promise<NpmCacheMeta | null> => {
	try {
		const content = await Bun.file(path.join(localPath, NPM_CACHE_META_FILE)).text();
		return JSON.parse(content) as NpmCacheMeta;
	} catch {
		return null;
	}
};

const shouldReuseCache = async (config: ResolvedNpmResourceArgs): Promise<boolean> => {
	const localPath = config.localPath;
	if (!config.requestedVersion || config.ephemeral) return false;
	const exists = await directoryExists(localPath);
	if (!exists) return false;

	const cached = await readCacheMeta(localPath);
	if (!cached) return false;
	return (
		cached.packageName === config.package &&
		cached.requestedVersion === config.requestedVersion &&
		cached.resolvedVersion.length > 0
	);
};

const installPackageFiles = async (
	args: {
		localPath: string;
		packageName: string;
		resolvedVersion: string;
	},
	deps: NpmResourceDeps
) => {
	const installDirectory = path.join(args.localPath, NPM_INSTALL_STAGING_DIR);
	const packagePath = path.join(installDirectory, 'node_modules', ...args.packageName.split('/'));

	try {
		await fs.mkdir(installDirectory, { recursive: true });
	} catch (cause) {
		throw new ResourceError({
			message: `Failed to prepare npm install workspace for "${args.packageName}"`,
			hint: 'Check that the btca data directory is writable.',
			cause
		});
	}

	try {
		await Bun.write(
			path.join(installDirectory, 'package.json'),
			JSON.stringify(
				{
					name: 'btca-npm-resource-install',
					private: true
				},
				null,
				2
			)
		);
	} catch (cause) {
		throw new ResourceError({
			message: `Failed to prepare npm install workspace for "${args.packageName}"`,
			hint: 'Check that the btca data directory is writable.',
			cause
		});
	}

	try {
		await runBunInstall(
			{
				installDirectory,
				packageName: args.packageName,
				resolvedVersion: args.resolvedVersion
			},
			deps
		);

		const hasInstalledPackage = await directoryExists(packagePath);
		if (!hasInstalledPackage) {
			throw new ResourceError({
				message: `Installed npm package directory is missing for "${args.packageName}@${args.resolvedVersion}"`,
				hint: 'Try again. If this keeps happening, the package may not publish source files.'
			});
		}

		try {
			await fs.cp(packagePath, args.localPath, { recursive: true, force: true });
		} catch (cause) {
			throw new ResourceError({
				message: `Failed to copy installed npm package files for "${args.packageName}"`,
				hint: 'Check filesystem permissions and available disk space.',
				cause
			});
		}
	} finally {
		await cleanupDirectory(installDirectory);
	}
};

const writeNpmMetadataFiles = async (args: {
	localPath: string;
	packageName: string;
	requestedVersion?: string;
	resolvedVersion: string;
	versionData: NpmPackageVersion;
	packageUrl: string;
	pageUrl: string;
	pageHtml: string;
}) => {
	const overview = formatResourceOverview({
		packageName: args.packageName,
		resolvedVersion: args.resolvedVersion,
		...(args.requestedVersion ? { requestedVersion: args.requestedVersion } : {}),
		packageUrl: args.packageUrl,
		pageUrl: args.pageUrl,
		versionData: args.versionData
	});
	const meta: NpmCacheMeta = {
		packageName: args.packageName,
		...(args.requestedVersion ? { requestedVersion: args.requestedVersion } : {}),
		resolvedVersion: args.resolvedVersion,
		packageUrl: args.packageUrl,
		pageUrl: args.pageUrl,
		fetchedAt: new Date().toISOString()
	};

	await Promise.all([
		Bun.write(path.join(args.localPath, NPM_CONTENT_FILE), overview),
		Bun.write(path.join(args.localPath, NPM_PAGE_FILE), args.pageHtml),
		Bun.write(path.join(args.localPath, NPM_CACHE_META_FILE), JSON.stringify(meta, null, 2))
	]);
};

const hydrateNpmResource = async (config: ResolvedNpmResourceArgs, deps: NpmResourceDeps) => {
	const localPath = config.localPath;
	const packagePath = encodePackagePath(config.package);
	const registryUrl = `${NPM_REGISTRY_HOST}/${encodeURIComponent(config.package)}`;
	const requestedVersion = config.requestedVersion?.trim();
	const packument = await fetchJson<NpmPackument>(registryUrl, config.name);
	const resolvedVersion = resolveRequestedVersion(packument, requestedVersion);

	if (!resolvedVersion) {
		throw new ResourceError({
			message: `Unable to resolve npm version for package "${config.package}"`,
			hint: requestedVersion
				? `Version/tag "${requestedVersion}" was not found. Try a valid version or tag like "latest".`
				: 'The package does not expose a resolvable latest version.'
		});
	}

	const versionData = packument.versions?.[resolvedVersion];
	if (!versionData) {
		throw new ResourceError({
			message: `NPM package metadata for "${config.package}@${resolvedVersion}" is missing`,
			hint: 'Try another version or run the command again.'
		});
	}

	const packageUrl = `https://www.npmjs.com/package/${packagePath}`;
	const pageUrl = `${packageUrl}/v/${encodeURIComponent(resolvedVersion)}`;
	const pageHtml = await fetchText(pageUrl, config.name);

	await installPackageFiles(
		{
			localPath,
			packageName: config.package,
			resolvedVersion
		},
		deps
	);

	await writeNpmMetadataFiles({
		localPath,
		packageName: config.package,
		...(requestedVersion ? { requestedVersion } : {}),
		resolvedVersion,
		versionData,
		packageUrl,
		pageUrl,
		pageHtml
	});
};

const ensureNpmResource = async (
	config: ResolvedNpmResourceArgs,
	deps: NpmResourceDeps
): Promise<string> => {
	const localPath = config.localPath;
	const basePath = path.dirname(localPath);

	try {
		await fs.mkdir(basePath, { recursive: true });
	} catch (cause) {
		throw new ResourceError({
			message: 'Failed to create resources directory',
			hint: 'Check that you have write permissions to the btca data directory.',
			cause
		});
	}

	const canReuse = await shouldReuseCache(config);
	if (canReuse) return localPath;

	await cleanupDirectory(localPath);
	try {
		await fs.mkdir(localPath, { recursive: true });
	} catch (cause) {
		throw new ResourceError({
			message: `Failed to prepare npm resource directory for "${config.name}"`,
			hint: 'Check that the btca data directory is writable.',
			cause
		});
	}

	await hydrateNpmResource(config, deps);
	return localPath;
};

export const loadNpmResource = async (
	config: BtcaNpmResourceArgs,
	deps?: Partial<NpmResourceDeps>
): Promise<BtcaNpmFsResource> => {
	const resolved = resolveNpmResourceArgs(config);
	const npmDeps = resolveNpmResourceDeps(deps);
	const cleanup = resolved.ephemeral
		? async () => {
				await withFilesystemLock(
					{
						lockPath: resolved.resourceLockPath,
						label: `npm.cleanup.${resolved.cacheKey}`,
						quiet: resolved.quiet
					},
					async () => {
						await cleanupDirectory(resolved.localPath);
					}
				);
			}
		: undefined;

	return {
		_tag: 'fs-based',
		name: resolved.name,
		fsName: resolved.fsName,
		type: 'npm',
		repoSubPaths: [],
		specialAgentInstructions: resolved.specialAgentInstructions,
		materializeIntoVirtualFs: async ({ destinationPath, vfsId }) => {
			return withClearAwareFilesystemLock(
				{
					clearLockPath: resolved.clearLockPath,
					clearLockWaitLabel: `wait-clear-before-npm.${resolved.cacheKey}`,
					clearLockInspectLabel: `inspect-clear-before-npm.${resolved.cacheKey}`,
					resourceLockPath: resolved.resourceLockPath,
					resourceLockLabel: `npm.${resolved.cacheKey}`,
					quiet: resolved.quiet
				},
				async () => {
					const materializedPath = await ensureNpmResource(resolved, npmDeps);
					await importDirectoryIntoVirtualFs({
						sourcePath: materializedPath,
						destinationPath,
						vfsId,
						ignore: shouldIgnoreCommonImportedPath
					});

					const cached = await readCacheMeta(materializedPath);
					return {
						metadata: {
							package: resolved.package,
							version: resolved.requestedVersion ?? cached?.resolvedVersion,
							url: cached?.packageUrl
						}
					};
				}
			);
		},
		...(cleanup ? { cleanup } : {})
	};
};
