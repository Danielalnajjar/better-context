import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readdir, rename, rm } from 'node:fs/promises';
import path from 'node:path';

import { Effect } from 'effect';

import type { ConfigService as ConfigServiceShape } from '../config/index.ts';
import { parseNpmReference, validateGitUrl } from '../validation/index.ts';
import { CommonHints } from '../errors.ts';
import { metricsInfo } from '../metrics/index.ts';

import { directoryExists, pathExists } from './fs-utils.ts';
import { ResourceError, resourceNameToKey } from './helpers.ts';
import { loadGitResource, type GitResourceDeps } from './impls/git.ts';
import { loadLocalResource } from './impls/local.ts';
import { loadNpmResource, type NpmResourceDeps } from './impls/npm.ts';
import {
	CLEAR_LOCK_NAME,
	CLEAR_TRASH_DIR,
	GIT_MIRRORS_DIR,
	RESOURCE_LOCKS_DIR,
	TMP_DIR,
	getClearLockPath,
	getClearTrashRoot,
	getGitLockPath,
	getGitMirrorPath,
	getGitMirrorRoot,
	getNpmLockPath,
	getResourceLocksRoot,
	getTmpCachePath,
	getTmpCacheRoot,
	getTopLevelCachePath
} from './layout.ts';
import { parseActiveResourceLockDirectoryName, withFilesystemLock } from './lock.ts';
import {
	isGitResource,
	isNpmResource,
	type ResourceDefinition,
	type GitResource,
	type LocalResource,
	type NpmResource
} from './schema.ts';
import type {
	BtcaFsResource,
	BtcaGitResourceArgs,
	BtcaLocalResourceArgs,
	BtcaNpmResourceArgs
} from './types.ts';

const ANON_PREFIX = 'anonymous:';
const ANON_DIRECTORY_PREFIX = 'anonymous-';
const ANONYMOUS_GIT_TMP_DIR_PREFIX = 'btca-anon-git-';
const ANONYMOUS_DIRECTORY_KEY_HASH = /^[0-9a-f]{12}$/u;
const DEFAULT_ANON_BRANCH = 'main';
const CLEAR_DELETE_MAX_RETRIES = 5;
const CLEAR_DELETE_RETRY_DELAY_MS = 100;
export const createAnonymousDirectoryKey = (reference: string): string => {
	const hash = createHash('sha256').update(reference).digest('hex').slice(0, 12);
	return `${ANON_DIRECTORY_PREFIX}${hash}`;
};

const isAnonymousResource = (name: string): boolean => name.startsWith(ANON_PREFIX);

export type ResourcesService = {
	load: (
		name: string,
		options?: {
			quiet?: boolean;
		}
	) => Effect.Effect<BtcaFsResource, ResourceError, never>;
	loadPromise: (
		name: string,
		options?: {
			quiet?: boolean;
		}
	) => Promise<BtcaFsResource>;
	clearCaches: () => Effect.Effect<{ cleared: number }, ResourceError, never>;
	clearCachesPromise: () => Promise<{ cleared: number }>;
};

type ResourcesServiceDeps = {
	readonly git?: Partial<GitResourceDeps>;
	readonly npm?: Partial<NpmResourceDeps>;
};

const normalizeSearchPaths = (definition: GitResource): string[] => {
	const paths = [
		...(definition.searchPaths ?? []),
		...(definition.searchPath ? [definition.searchPath] : [])
	];
	return paths.filter((path) => path.trim().length > 0);
};

const definitionToGitArgs = (
	definition: GitResource,
	resourcesDirectory: string,
	quiet: boolean
): BtcaGitResourceArgs => ({
	type: 'git',
	name: definition.name,
	url: definition.url,
	branch: definition.branch,
	repoSubPaths: normalizeSearchPaths(definition),
	resourcesDirectoryPath: resourcesDirectory,
	specialAgentInstructions: definition.specialNotes ?? '',
	quiet,
	ephemeral: isAnonymousResource(definition.name),
	localDirectoryKey: isAnonymousResource(definition.name)
		? createAnonymousDirectoryKey(definition.url)
		: undefined
});

const definitionToLocalArgs = (definition: LocalResource): BtcaLocalResourceArgs => ({
	type: 'local',
	name: definition.name,
	path: definition.path,
	specialAgentInstructions: definition.specialNotes ?? ''
});

const definitionToNpmArgs = (
	definition: NpmResource,
	resourcesDirectory: string,
	quiet: boolean
): BtcaNpmResourceArgs => {
	const reference = `${definition.package}${definition.version ? `@${definition.version}` : ''}`;
	return {
		type: 'npm',
		name: definition.name,
		package: definition.package,
		...(definition.version ? { version: definition.version } : {}),
		resourcesDirectoryPath: resourcesDirectory,
		specialAgentInstructions: definition.specialNotes ?? '',
		quiet,
		ephemeral: isAnonymousResource(definition.name),
		localDirectoryKey: isAnonymousResource(definition.name)
			? createAnonymousDirectoryKey(reference)
			: undefined
	};
};

export const createAnonymousResource = (reference: string): ResourceDefinition | null => {
	const npmReference = parseNpmReference(reference);
	if (npmReference) {
		return {
			type: 'npm',
			name: `${ANON_PREFIX}${npmReference.normalizedReference}`,
			package: npmReference.packageName,
			...(npmReference.version ? { version: npmReference.version } : {})
		};
	}

	const gitUrlResult = validateGitUrl(reference);
	if (gitUrlResult.valid) {
		const normalizedUrl = gitUrlResult.value;
		return {
			type: 'git',
			name: `${ANON_PREFIX}${normalizedUrl}`,
			url: normalizedUrl,
			branch: DEFAULT_ANON_BRANCH
		};
	}
	return null;
};

export const resolveResourceDefinition = (
	reference: string,
	getResource: ConfigServiceShape['getResource']
): ResourceDefinition => {
	const definition = getResource(reference);
	if (definition) return definition;

	const anonymousDefinition = createAnonymousResource(reference);
	if (anonymousDefinition) return anonymousDefinition;

	throw new ResourceError({
		message: `Resource "${reference}" not found in config`,
		hint: `${CommonHints.LIST_RESOURCES} ${CommonHints.ADD_RESOURCE}`
	});
};

const removeDirectoryWithRetries = async (targetPath: string) => {
	await rm(targetPath, {
		recursive: true,
		force: true,
		maxRetries: CLEAR_DELETE_MAX_RETRIES,
		retryDelay: CLEAR_DELETE_RETRY_DELAY_MS
	});
};

const moveDirectoryToClearTrashIfExists = async (
	resourcesDirectory: string,
	targetPath: string
) => {
	if (!(await pathExists(targetPath))) return null;

	const trashRoot = getClearTrashRoot(resourcesDirectory);
	await mkdir(trashRoot, { recursive: true });
	const trashPath = path.join(
		trashRoot,
		`${path.basename(targetPath)}-${Date.now()}-${randomUUID()}`
	);

	try {
		await rename(targetPath, trashPath);
		return trashPath;
	} catch (cause) {
		const code =
			typeof cause === 'object' && cause && 'code' in cause
				? (cause as { code?: string }).code
				: '';
		if (code === 'ENOENT') return null;
		throw cause;
	}
};

const removeClearedTrashPaths = async (trashPaths: readonly string[]) => {
	for (const trashPath of trashPaths) {
		await removeDirectoryWithRetries(trashPath);
	}
	return trashPaths.length;
};

const listDirectoryEntriesSorted = async (directoryPath: string) => {
	try {
		return (await readdir(directoryPath)).sort((left, right) => left.localeCompare(right));
	} catch {
		return [];
	}
};

const getCurrentNamedGitKeys = (resources: readonly ResourceDefinition[]) =>
	resources
		.filter((resource): resource is GitResource => isGitResource(resource))
		.map((resource) => resourceNameToKey(resource.name))
		.sort((left, right) => left.localeCompare(right));

const getCurrentNamedNpmKeys = (resources: readonly ResourceDefinition[]) =>
	resources
		.filter((resource): resource is NpmResource => isNpmResource(resource))
		.map((resource) => resourceNameToKey(resource.name))
		.sort((left, right) => left.localeCompare(right));

const getExtraActiveLockKeys = async (
	resourcesDirectory: string,
	currentKeys: {
		currentNamedGitKeys: readonly string[];
		currentNamedNpmKeys: readonly string[];
	}
) => {
	const lockEntries = await listDirectoryEntriesSorted(getResourceLocksRoot(resourcesDirectory));
	const extraGitKeys = new Set<string>();
	const extraNpmKeys = new Set<string>();
	const currentGit = new Set(currentKeys.currentNamedGitKeys);
	const currentNpm = new Set(currentKeys.currentNamedNpmKeys);

	for (const entry of lockEntries) {
		if (entry === CLEAR_LOCK_NAME) continue;
		const parsed = parseActiveResourceLockDirectoryName(entry);
		if (!parsed) continue;
		if (parsed.namespace === 'git') {
			if (!currentGit.has(parsed.key)) extraGitKeys.add(parsed.key);
			continue;
		}
		if (!currentNpm.has(parsed.key)) extraNpmKeys.add(parsed.key);
	}

	return {
		extraGitKeys: [...extraGitKeys].sort((left, right) => left.localeCompare(right)),
		extraNpmKeys: [...extraNpmKeys].sort((left, right) => left.localeCompare(right))
	};
};

const withGitDrain = async <T>(resourcesDirectory: string, key: string, drain: () => Promise<T>) =>
	withFilesystemLock(
		{
			lockPath: getGitLockPath(resourcesDirectory, key),
			label: `clear.git.${key}`,
			quiet: true
		},
		drain
	);

const parseAnonymousGitTmpDirectoryKey = (entry: string) => {
	const prefix = `${ANONYMOUS_GIT_TMP_DIR_PREFIX}${ANON_DIRECTORY_PREFIX}`;
	if (!entry.startsWith(prefix)) return null;

	const remainder = entry.slice(prefix.length);
	const separatorIndex = remainder.indexOf('-');
	if (separatorIndex <= 0) return null;

	const hash = remainder.slice(0, separatorIndex);
	if (!ANONYMOUS_DIRECTORY_KEY_HASH.test(hash)) return null;

	return `${ANON_DIRECTORY_PREFIX}${hash}`;
};

const withNpmDrain = async <T>(resourcesDirectory: string, key: string, drain: () => Promise<T>) =>
	withFilesystemLock(
		{
			lockPath: getNpmLockPath(resourcesDirectory, key),
			label: `clear.npm.${key}`,
			quiet: true
		},
		drain
	);

const classifyTopLevelCache = async (cachePath: string) => {
	if (!(await directoryExists(cachePath))) return 'skip' as const;
	if (await pathExists(path.join(cachePath, '.btca-npm-meta.json'))) return 'npm' as const;
	if (await pathExists(path.join(cachePath, '.git'))) return 'git' as const;
	return 'unknown' as const;
};

const recordHandled = (handledPaths: Set<string>, ...identities: string[]) => {
	for (const identity of identities) {
		handledPaths.add(identity);
	}
};

const drainGitMirrorKey = async (resourcesDirectory: string, key: string) =>
	removeClearedTrashPaths(
		(
			await withGitDrain(resourcesDirectory, key, async () => {
				const trashPath = await moveDirectoryToClearTrashIfExists(
					resourcesDirectory,
					getGitMirrorPath(resourcesDirectory, key)
				);
				return trashPath ? [trashPath] : [];
			})
		).filter(Boolean)
	);

const drainAnonymousGitTmpDirectories = async (
	resourcesDirectory: string,
	key: string,
	entries: readonly string[]
) =>
	removeClearedTrashPaths(
		await withGitDrain(resourcesDirectory, key, async () => {
			const trashPaths: string[] = [];
			for (const entry of entries) {
				const trashPath = await moveDirectoryToClearTrashIfExists(
					resourcesDirectory,
					path.join(getTmpCacheRoot(resourcesDirectory), entry)
				);
				if (trashPath) {
					trashPaths.push(trashPath);
				}
			}
			return trashPaths;
		})
	);

const drainNpmCacheKey = async (
	resourcesDirectory: string,
	key: string,
	options: {
		readonly includeTopLevel?: boolean;
		readonly includeTmp?: boolean;
	}
) =>
	removeClearedTrashPaths(
		await withNpmDrain(resourcesDirectory, key, async () => {
			const trashPaths: string[] = [];
			if (options.includeTopLevel !== false) {
				const trashPath = await moveDirectoryToClearTrashIfExists(
					resourcesDirectory,
					getTopLevelCachePath(resourcesDirectory, key)
				);
				if (trashPath) {
					trashPaths.push(trashPath);
				}
			}
			if (options.includeTmp === true) {
				const trashPath = await moveDirectoryToClearTrashIfExists(
					resourcesDirectory,
					getTmpCachePath(resourcesDirectory, key)
				);
				if (trashPath) {
					trashPaths.push(trashPath);
				}
			}
			return trashPaths;
		})
	);

const sweepRemainingGitMirrors = async (resourcesDirectory: string, handledPaths: Set<string>) => {
	let cleared = 0;
	for (const key of await listDirectoryEntriesSorted(getGitMirrorRoot(resourcesDirectory))) {
		const identity = `mirror:${key}`;
		if (handledPaths.has(identity)) continue;
		recordHandled(handledPaths, identity);
		cleared += await drainGitMirrorKey(resourcesDirectory, key);
	}
	return cleared;
};

const sweepRemainingTopLevelCaches = async (
	resourcesDirectory: string,
	handledPaths: Set<string>
) => {
	let cleared = 0;
	for (const key of await listDirectoryEntriesSorted(resourcesDirectory)) {
		if ([CLEAR_TRASH_DIR, GIT_MIRRORS_DIR, RESOURCE_LOCKS_DIR, TMP_DIR].includes(key)) continue;

		const identity = `top:${key}`;
		if (handledPaths.has(identity)) continue;

		const cachePath = getTopLevelCachePath(resourcesDirectory, key);
		const classification = await classifyTopLevelCache(cachePath);
		if (classification === 'skip') continue;

		if (classification === 'npm') {
			recordHandled(handledPaths, identity);
			cleared += await drainNpmCacheKey(resourcesDirectory, key, { includeTopLevel: true });
			continue;
		}

		if (classification === 'git') {
			recordHandled(handledPaths, identity);
			cleared += await removeClearedTrashPaths(
				(
					await withGitDrain(resourcesDirectory, key, async () => {
						const trashPath = await moveDirectoryToClearTrashIfExists(
							resourcesDirectory,
							cachePath
						);
						return trashPath ? [trashPath] : [];
					})
				).filter(Boolean)
			);
			continue;
		}

		const trashPath = await moveDirectoryToClearTrashIfExists(resourcesDirectory, cachePath);
		if (trashPath) {
			recordHandled(handledPaths, identity);
			cleared += await removeClearedTrashPaths([trashPath]);
			metricsInfo('resources.clear.unknown_legacy_cache', {
				path: cachePath,
				key
			});
		}
	}
	return cleared;
};

const sweepClearTrashRoot = async (resourcesDirectory: string) => {
	for (const entry of await listDirectoryEntriesSorted(getClearTrashRoot(resourcesDirectory))) {
		await removeDirectoryWithRetries(path.join(getClearTrashRoot(resourcesDirectory), entry));
	}
};

const sweepRemainingTmpCaches = async (resourcesDirectory: string, handledPaths: Set<string>) => {
	let cleared = 0;
	const anonymousGitTmpEntriesByKey = new Map<string, string[]>();
	const npmTmpKeys: string[] = [];

	for (const entry of await listDirectoryEntriesSorted(getTmpCacheRoot(resourcesDirectory))) {
		const identity = `tmp:${entry}`;
		if (handledPaths.has(identity)) continue;
		recordHandled(handledPaths, identity);

		const anonymousGitKey = parseAnonymousGitTmpDirectoryKey(entry);
		if (anonymousGitKey) {
			const entries = anonymousGitTmpEntriesByKey.get(anonymousGitKey) ?? [];
			entries.push(entry);
			anonymousGitTmpEntriesByKey.set(anonymousGitKey, entries);
			continue;
		}

		npmTmpKeys.push(entry);
	}

	for (const [key, entries] of anonymousGitTmpEntriesByKey) {
		cleared += await drainAnonymousGitTmpDirectories(resourcesDirectory, key, entries);
	}

	for (const key of npmTmpKeys) {
		cleared += await drainNpmCacheKey(resourcesDirectory, key, {
			includeTopLevel: false,
			includeTmp: true
		});
	}

	return cleared;
};

export const createResourcesService = (
	config: ConfigServiceShape,
	deps?: ResourcesServiceDeps
): ResourcesService => {
	const normalizeClearError = (cause: unknown) =>
		cause instanceof ResourceError
			? cause
			: new ResourceError({
					message: 'Failed to clear BTCA-managed resource caches',
					hint: 'Check filesystem permissions for the BTCA data directory and try again.',
					cause
				});

	const loadPromise: ResourcesService['loadPromise'] = async (name, options) => {
		const quiet = options?.quiet ?? false;
		const definition = resolveResourceDefinition(name, config.getResource);

		if (isGitResource(definition)) {
			try {
				return await loadGitResource(
					definitionToGitArgs(definition, config.resourcesDirectory, quiet),
					deps?.git
				);
			} catch (cause) {
				if (cause instanceof ResourceError) throw cause;
				throw new ResourceError({
					message: `Failed to load git resource "${name}"`,
					hint: CommonHints.CLEAR_CACHE,
					cause
				});
			}
		}

		if (isNpmResource(definition)) {
			try {
				return await loadNpmResource(
					definitionToNpmArgs(definition, config.resourcesDirectory, quiet),
					deps?.npm
				);
			} catch (cause) {
				if (cause instanceof ResourceError) throw cause;
				throw new ResourceError({
					message: `Failed to load npm resource "${name}"`,
					hint: CommonHints.CLEAR_CACHE,
					cause
				});
			}
		}

		return loadLocalResource(definitionToLocalArgs(definition));
	};

	const clearCachesPromise: ResourcesService['clearCachesPromise'] = async () => {
		const lockPath = getClearLockPath(config.resourcesDirectory);
		try {
			return await withFilesystemLock(
				{
					lockPath,
					label: 'resources.clear',
					quiet: true
				},
				async () => {
					let clearedCount = 0;
					const handledPaths = new Set<string>();
					const currentNamedGitKeys = getCurrentNamedGitKeys(config.resources);
					const currentNamedNpmKeys = getCurrentNamedNpmKeys(config.resources);
					const { extraGitKeys, extraNpmKeys } = await getExtraActiveLockKeys(
						config.resourcesDirectory,
						{
							currentNamedGitKeys,
							currentNamedNpmKeys
						}
					);

					for (const key of currentNamedGitKeys) {
						recordHandled(handledPaths, `mirror:${key}`);
						clearedCount += await drainGitMirrorKey(config.resourcesDirectory, key);
					}

					for (const key of currentNamedNpmKeys) {
						recordHandled(handledPaths, `top:${key}`);
						clearedCount += await drainNpmCacheKey(config.resourcesDirectory, key, {
							includeTopLevel: true
						});
					}

					for (const key of extraGitKeys) {
						recordHandled(handledPaths, `mirror:${key}`);
						clearedCount += await drainGitMirrorKey(config.resourcesDirectory, key);
					}

					for (const key of extraNpmKeys) {
						recordHandled(handledPaths, `top:${key}`, `tmp:${key}`);
						clearedCount += await drainNpmCacheKey(config.resourcesDirectory, key, {
							includeTopLevel: true,
							includeTmp: true
						});
					}

					clearedCount += await sweepRemainingGitMirrors(config.resourcesDirectory, handledPaths);
					clearedCount += await sweepRemainingTopLevelCaches(
						config.resourcesDirectory,
						handledPaths
					);
					clearedCount += await sweepRemainingTmpCaches(config.resourcesDirectory, handledPaths);
					await sweepClearTrashRoot(config.resourcesDirectory);

					await mkdir(getResourceLocksRoot(config.resourcesDirectory), { recursive: true });

					return { cleared: clearedCount };
				}
			);
		} catch (cause) {
			throw normalizeClearError(cause);
		}
	};

	const load: ResourcesService['load'] = (name, options) =>
		Effect.tryPromise({
			try: () => loadPromise(name, options),
			catch: (cause) =>
				cause instanceof ResourceError
					? cause
					: new ResourceError({
							message: `Failed to resolve resource "${name}"`,
							hint: `${CommonHints.LIST_RESOURCES} ${CommonHints.ADD_RESOURCE}`,
							cause
						})
		});

	return {
		load,
		loadPromise,
		clearCaches: () =>
			Effect.tryPromise({
				try: () => clearCachesPromise(),
				catch: normalizeClearError
			}),
		clearCachesPromise
	};
};
