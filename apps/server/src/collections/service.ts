import path from 'node:path';

import { Effect } from 'effect';

import type { ConfigService as ConfigServiceShape } from '../config/index.ts';
import { runTransaction } from '../context/transaction.ts';
import { CommonHints, getErrorHint, getErrorMessage } from '../errors.ts';
import { metricsInfo } from '../metrics/index.ts';
import type { ResourcesService } from '../resources/service.ts';
import { isGitResource, isNpmResource } from '../resources/schema.ts';
import { FS_RESOURCE_SYSTEM_NOTE, type BtcaFsResource } from '../resources/types.ts';
import { parseNpmReference } from '../validation/index.ts';
import { CollectionError, getCollectionKey, type CollectionResult } from './types.ts';
import {
	createVirtualFs,
	disposeVirtualFs,
	importDirectoryIntoVirtualFs,
	importPathsIntoVirtualFs,
	mkdirVirtualFs,
	rmVirtualFs
} from '../vfs/virtual-fs.ts';
import {
	clearVirtualCollectionMetadata,
	setVirtualCollectionMetadata,
	type VirtualResourceMetadata
} from './virtual-metadata.ts';

export type CollectionsService = {
	load: (args: {
		resourceNames: readonly string[];
		quiet?: boolean;
	}) => Effect.Effect<CollectionResult, CollectionError, never>;
	loadPromise: (args: {
		resourceNames: readonly string[];
		quiet?: boolean;
	}) => Promise<CollectionResult>;
};

const escapeXml = (value: string) =>
	value
		.replaceAll('&', '&amp;')
		.replaceAll('<', '&lt;')
		.replaceAll('>', '&gt;')
		.replaceAll('"', '&quot;')
		.replaceAll("'", '&apos;');

const getResourceTypeLabel = (resource: BtcaFsResource) => {
	if (resource.type === 'git') return 'git repo';
	if (resource.type === 'npm') return 'npm package';
	return 'local directory';
};

const trimGitSuffix = (url: string) => url.replace(/\.git$/u, '').replace(/\/+$/u, '');

const encodePathSegments = (value: string) => value.split('/').map(encodeURIComponent).join('/');

const xmlLine = (tag: string, value?: string) =>
	value ? `\t\t<${tag}>${escapeXml(value)}</${tag}>` : '';

const xmlPathBlock = (tag: string, values: readonly string[], prefix = '') =>
	values.length === 0
		? ''
		: [
				`\t\t<${tag}>`,
				...values.map((value) => `\t\t\t<path>${escapeXml(`${prefix}${value}`)}</path>`),
				`\t\t</${tag}>`
			].join('\n');

const getNpmCitationAlias = (metadata?: VirtualResourceMetadata) => {
	if (!metadata?.package) return undefined;
	return `npm:${metadata.package}@${metadata.version ?? 'latest'}`;
};

const getNpmFileUrlPrefix = (metadata?: VirtualResourceMetadata) => {
	if (!metadata?.package || !metadata?.version) return undefined;
	return `https://unpkg.com/${metadata.package}@${metadata.version}`;
};

const createCollectionInstructionBlock = (
	resource: BtcaFsResource,
	metadata?: VirtualResourceMetadata
) => {
	const repoUrl =
		resource.type === 'git' && metadata?.url ? trimGitSuffix(metadata.url) : undefined;
	const gitRef = metadata?.branch ?? metadata?.commit;
	const githubBlobPrefix =
		repoUrl && gitRef ? `${repoUrl}/blob/${encodeURIComponent(gitRef)}` : undefined;
	const npmCitationAlias = resource.type === 'npm' ? getNpmCitationAlias(metadata) : undefined;
	const npmFileUrlPrefix = resource.type === 'npm' ? getNpmFileUrlPrefix(metadata) : undefined;

	return [
		'\t<resource>',
		`\t\t<name>${escapeXml(resource.name)}</name>`,
		`\t\t<type>${getResourceTypeLabel(resource)}</type>`,
		`\t\t<system_note>${escapeXml(FS_RESOURCE_SYSTEM_NOTE)}</system_note>`,
		`\t\t<path>${escapeXml(`./${resource.fsName}`)}</path>`,
		xmlLine('repo_url', repoUrl),
		xmlLine('repo_branch', resource.type === 'git' ? metadata?.branch : undefined),
		xmlLine('repo_commit', resource.type === 'git' ? metadata?.commit : undefined),
		xmlLine('npm_package', resource.type === 'npm' ? metadata?.package : undefined),
		xmlLine('npm_version', resource.type === 'npm' ? metadata?.version : undefined),
		xmlLine('npm_url', resource.type === 'npm' ? metadata?.url : undefined),
		xmlLine('npm_citation_alias', npmCitationAlias),
		xmlLine('npm_file_url_prefix', npmFileUrlPrefix),
		xmlLine('github_blob_prefix', githubBlobPrefix),
		xmlLine(
			'citation_rule',
			githubBlobPrefix
				? `Convert virtual paths under ./${resource.fsName}/ to repo-relative paths, then encode each path segment for GitHub URLs.`
				: resource.type === 'npm' && npmCitationAlias
					? `In Sources, cite npm files using ${npmCitationAlias}/<file> and link them to ${npmFileUrlPrefix ?? 'the exact file URL prefix'}/<file>. Do not cite encoded virtual folder names.`
					: 'Cite local file paths only for this resource.'
		),
		xmlLine(
			'citation_example',
			githubBlobPrefix
				? `${githubBlobPrefix}/${encodePathSegments('src/routes/blog/+page.server.js')}`
				: resource.type === 'npm' && npmCitationAlias && npmFileUrlPrefix
					? `${npmFileUrlPrefix}/package.json`
					: undefined
		),
		xmlPathBlock('focus_paths', resource.repoSubPaths, `./${resource.fsName}/`),
		xmlLine('special_notes', resource.specialAgentInstructions),
		'\t</resource>'
	]
		.filter(Boolean)
		.join('\n');
};

const createCollectionInstructions = (
	resources: readonly BtcaFsResource[],
	metadataResources: readonly VirtualResourceMetadata[]
) => {
	const metadataByName = new Map(metadataResources.map((resource) => [resource.name, resource]));

	return [
		'<available_resources>',
		...resources.map((resource) =>
			createCollectionInstructionBlock(resource, metadataByName.get(resource.name))
		),
		'</available_resources>'
	].join('\n');
};

const ignoreErrors = async (action: () => Promise<unknown>) => {
	try {
		await action();
	} catch {
		return;
	}
};

const LOCAL_RESOURCE_IGNORED_DIRECTORIES = new Set([
	'.git',
	'.turbo',
	'.next',
	'.svelte-kit',
	'.vercel',
	'.cache',
	'coverage',
	'dist',
	'build',
	'out',
	'node_modules'
]);

const normalizeRelativePath = (value: string) => value.split(path.sep).join('/');

const shouldIgnoreImportedPath = (resource: BtcaFsResource, relativePath: string) => {
	const normalized = normalizeRelativePath(relativePath);
	if (!normalized || normalized === '.') return false;
	const segments = normalized.split('/');
	if (segments.includes('.git')) return true;
	if (resource.type !== 'local') return false;
	return segments.some((segment) => LOCAL_RESOURCE_IGNORED_DIRECTORIES.has(segment));
};

const listGitVisiblePaths = async (resourcePath: string) => {
	try {
		const proc = Bun.spawn(
			['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'],
			{
				cwd: resourcePath,
				stdout: 'pipe',
				stderr: 'ignore'
			}
		);
		const stdout = await new Response(proc.stdout).text();
		const exitCode = await proc.exited;
		if (exitCode !== 0) return null;
		return stdout
			.split('\0')
			.map((entry) => entry.trim())
			.filter((entry) => entry.length > 0);
	} catch {
		return null;
	}
};

const initVirtualRoot = async (collectionPath: string, vfsId: string) => {
	try {
		await mkdirVirtualFs(collectionPath, { recursive: true }, vfsId);
	} catch (cause) {
		throw new CollectionError({
			message: `Failed to initialize virtual collection root: "${collectionPath}"`,
			hint: 'Check that the virtual filesystem is available.',
			cause
		});
	}
};

const loadResource = async (resources: ResourcesService, name: string, quiet: boolean) => {
	try {
		return await resources.loadPromise(name, { quiet });
	} catch (cause) {
		const underlyingHint = getErrorHint(cause);
		const underlyingMessage = getErrorMessage(cause);
		throw new CollectionError({
			message: `Failed to load resource "${name}": ${underlyingMessage}`,
			hint:
				underlyingHint ??
				`${CommonHints.CLEAR_CACHE} Check that the resource "${name}" is correctly configured.`,
			cause
		});
	}
};

const resolveResourcePath = async (resource: BtcaFsResource) => {
	try {
		return await resource.getAbsoluteDirectoryPath();
	} catch (cause) {
		throw new CollectionError({
			message: `Failed to get path for resource "${resource.name}"`,
			hint: CommonHints.CLEAR_CACHE,
			cause
		});
	}
};

const virtualizeResource = async (args: {
	resource: BtcaFsResource;
	resourcePath: string;
	virtualResourcePath: string;
	vfsId: string;
}) => {
	try {
		if (args.resource.type === 'local') {
			const gitVisiblePaths = await listGitVisiblePaths(args.resourcePath);
			if (gitVisiblePaths) {
				await importPathsIntoVirtualFs({
					sourcePath: args.resourcePath,
					destinationPath: args.virtualResourcePath,
					relativePaths: gitVisiblePaths,
					vfsId: args.vfsId
				});
				return;
			}
		}

		await importDirectoryIntoVirtualFs({
			sourcePath: args.resourcePath,
			destinationPath: args.virtualResourcePath,
			vfsId: args.vfsId,
			ignore: (relativePath) => shouldIgnoreImportedPath(args.resource, relativePath)
		});
	} catch (cause) {
		throw new CollectionError({
			message: `Failed to virtualize resource "${args.resource.name}"`,
			hint: CommonHints.CLEAR_CACHE,
			cause
		});
	}
};

const getGitHeadHash = async (resourcePath: string) => {
	try {
		const proc = Bun.spawn(['git', 'rev-parse', 'HEAD'], {
			cwd: resourcePath,
			stdout: 'pipe',
			stderr: 'pipe'
		});
		const stdout = await new Response(proc.stdout).text();
		const exitCode = await proc.exited;
		if (exitCode !== 0) return undefined;
		const trimmed = stdout.trim();
		return trimmed.length > 0 ? trimmed : undefined;
	} catch {
		return undefined;
	}
};

const getGitHeadBranch = async (resourcePath: string) => {
	try {
		const proc = Bun.spawn(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], {
			cwd: resourcePath,
			stdout: 'pipe',
			stderr: 'pipe'
		});
		const stdout = await new Response(proc.stdout).text();
		const exitCode = await proc.exited;
		if (exitCode !== 0) return undefined;
		const trimmed = stdout.trim();
		if (!trimmed || trimmed === 'HEAD') return undefined;
		return trimmed;
	} catch {
		return undefined;
	}
};

const ANON_PREFIX = 'anonymous:';
const getAnonymousUrlFromName = (name: string) =>
	name.startsWith(ANON_PREFIX) ? name.slice(ANON_PREFIX.length) : undefined;
const NPM_ANON_PREFIX = `${ANON_PREFIX}npm:`;
const NPM_META_FILE = '.btca-npm-meta.json';
const getAnonymousNpmReferenceFromName = (name: string) =>
	name.startsWith(NPM_ANON_PREFIX) ? name.slice(ANON_PREFIX.length) : undefined;

const readNpmMeta = async (resourcePath: string) => {
	try {
		const content = await Bun.file(path.join(resourcePath, NPM_META_FILE)).text();
		return JSON.parse(content) as {
			packageName?: string;
			resolvedVersion?: string;
			packageUrl?: string;
		};
	} catch {
		return null;
	}
};

const buildVirtualMetadata = async (args: {
	resource: BtcaFsResource;
	resourcePath: string;
	loadedAt: string;
	definition?: ReturnType<ConfigServiceShape['getResource']>;
}) => {
	const base = {
		name: args.resource.name,
		fsName: args.resource.fsName,
		type: args.resource.type,
		path: args.resourcePath,
		repoSubPaths: args.resource.repoSubPaths,
		loadedAt: args.loadedAt
	};

	if (args.resource.type === 'npm') {
		const configuredDefinition =
			args.definition && isNpmResource(args.definition) ? args.definition : null;
		const anonymousReference = getAnonymousNpmReferenceFromName(args.resource.name);
		const anonymousNpm = anonymousReference ? parseNpmReference(anonymousReference) : null;
		const cached = await readNpmMeta(args.resourcePath);
		const packageName =
			configuredDefinition?.package ?? cached?.packageName ?? anonymousNpm?.packageName;
		const version =
			configuredDefinition?.version ?? cached?.resolvedVersion ?? anonymousNpm?.version;
		const url = cached?.packageUrl ?? anonymousNpm?.packageUrl;

		return {
			...base,
			...(packageName ? { package: packageName } : {}),
			...(version ? { version } : {}),
			...(url ? { url } : {})
		};
	}

	if (args.resource.type !== 'git') return base;

	const configuredDefinition =
		args.definition && isGitResource(args.definition) ? args.definition : null;
	const url = configuredDefinition?.url ?? getAnonymousUrlFromName(args.resource.name);
	const branch = configuredDefinition?.branch ?? (await getGitHeadBranch(args.resourcePath));
	const commit = await getGitHeadHash(args.resourcePath);

	return {
		...base,
		...(url ? { url } : {}),
		...(branch ? { branch } : {}),
		...(commit ? { commit } : {})
	};
};

export const createCollectionsService = (args: {
	config: ConfigServiceShape;
	resources: ResourcesService;
}): CollectionsService => {
	const loadPromise: CollectionsService['loadPromise'] = ({ resourceNames, quiet = false }) =>
		runTransaction('collections.load', async () => {
			const uniqueNames = Array.from(new Set(resourceNames));
			if (uniqueNames.length === 0)
				throw new CollectionError({
					message: 'Cannot create collection with no resources',
					hint: `${CommonHints.LIST_RESOURCES} ${CommonHints.ADD_RESOURCE}`
				});

			metricsInfo('collections.load', { resources: uniqueNames, quiet });

			const sortedNames = [...uniqueNames].sort((a, b) => a.localeCompare(b));
			const key = getCollectionKey(sortedNames);
			const collectionPath = '/';
			const vfsId = createVirtualFs();
			const cleanupVirtual = () => {
				disposeVirtualFs(vfsId);
				clearVirtualCollectionMetadata(vfsId);
			};
			const cleanupResources = (resources: BtcaFsResource[]) =>
				Promise.all(
					resources.map(async (resource) => {
						if (!resource.cleanup) return;
						await ignoreErrors(() => resource.cleanup!());
					})
				);

			const loadedResources: BtcaFsResource[] = [];

			try {
				await initVirtualRoot(collectionPath, vfsId);

				for (const name of sortedNames) {
					const resource = await loadResource(args.resources, name, quiet);
					loadedResources.push(resource);
				}

				const metadataResources: VirtualResourceMetadata[] = [];
				const loadedAt = new Date().toISOString();
				for (const resource of loadedResources) {
					const resourcePath = await resolveResourcePath(resource);
					const virtualResourcePath = path.posix.join('/', resource.fsName);

					await ignoreErrors(() =>
						rmVirtualFs(virtualResourcePath, { recursive: true, force: true }, vfsId)
					);

					await virtualizeResource({
						resource,
						resourcePath,
						virtualResourcePath,
						vfsId
					});

					const definition = args.config.getResource(resource.name);
					const metadata = await buildVirtualMetadata({
						resource,
						resourcePath,
						loadedAt,
						definition
					});
					if (metadata) metadataResources.push(metadata);
				}

				setVirtualCollectionMetadata({
					vfsId,
					collectionKey: key,
					createdAt: loadedAt,
					resources: metadataResources
				});

				return {
					path: collectionPath,
					agentInstructions: createCollectionInstructions(loadedResources, metadataResources),
					vfsId,
					cleanup: async () => {
						await cleanupResources(loadedResources);
					}
				};
			} catch (cause) {
				cleanupVirtual();
				await cleanupResources(loadedResources);
				if (cause instanceof CollectionError) throw cause;
				throw new CollectionError({
					message: 'Failed to load resource collection',
					hint: CommonHints.CLEAR_CACHE,
					cause
				});
			}
		});

	const load: CollectionsService['load'] = ({ resourceNames, quiet }) =>
		Effect.tryPromise({
			try: () => loadPromise({ resourceNames, quiet }),
			catch: (cause) =>
				cause instanceof CollectionError
					? cause
					: new CollectionError({
							message: 'Failed to load resource collection',
							hint: CommonHints.CLEAR_CACHE,
							cause
						})
		});

	return {
		load,
		loadPromise
	};
};
