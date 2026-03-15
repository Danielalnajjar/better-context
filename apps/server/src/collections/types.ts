import type { TaggedErrorOptions } from '../errors.ts';
import { resourceNameToKey } from '../resources/helpers.ts';

export type CollectionResult = {
	path: string;
	agentInstructions: string;
	vfsId?: string;
	cleanup?: () => Promise<void>;
};

export type CollectionErrorCode = 'RESOURCE_LOAD_FAILED' | 'RESOURCE_MATERIALIZE_FAILED';

export class CollectionError extends Error {
	readonly _tag = 'CollectionError';
	declare readonly cause?: unknown;
	readonly hint?: string;
	readonly code?: CollectionErrorCode;
	readonly resourceName?: string;

	constructor(
		args: TaggedErrorOptions & {
			code?: CollectionErrorCode;
			resourceName?: string;
		}
	) {
		super(args.message, args.cause === undefined ? undefined : { cause: args.cause });
		this.hint = args.hint;
		this.code = args.code;
		this.resourceName = args.resourceName;
	}
}

export const getCollectionKey = (resourceNames: readonly string[]): string => {
	return [...resourceNames].map(resourceNameToKey).sort().join('+');
};
