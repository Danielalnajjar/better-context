import type { TaggedErrorOptions } from '../errors.ts';

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

export class ResourceError extends Error {
	readonly _tag = 'ResourceError';
	override readonly cause?: unknown;
	readonly hint?: string;

	constructor(args: TaggedErrorOptions) {
		super(args.message);
		this.cause = args.cause;
		this.hint = args.hint;
		if (args.stack) this.stack = args.stack;
	}
}

export const resourceNameToKey = (name: string): string => {
	return encodeURIComponent(name);
};

const normalizeRelativePath = (value: string) => value.split('\\').join('/');

export const shouldIgnoreCommonImportedPath = (relativePath: string): boolean => {
	const normalized = normalizeRelativePath(relativePath);
	if (!normalized || normalized === '.') return false;
	return normalized.split('/').includes('.git');
};

export const shouldIgnoreLocalImportedPath = (relativePath: string): boolean => {
	const normalized = normalizeRelativePath(relativePath);
	if (!normalized || normalized === '.') return false;
	if (shouldIgnoreCommonImportedPath(normalized)) return true;
	return normalized.split('/').some((segment) => LOCAL_RESOURCE_IGNORED_DIRECTORIES.has(segment));
};
