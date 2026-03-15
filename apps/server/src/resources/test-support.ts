import { promises as fs } from 'node:fs';
import path from 'node:path';

import { Effect } from 'effect';

import type { ConfigService } from '../config/index.ts';

import type { ResourceDefinition } from './schema.ts';

export const createConfigServiceMock = (args: {
	readonly resourcesDirectory: string;
	readonly resources: readonly ResourceDefinition[];
	readonly configPath?: string;
}) =>
	({
		resourcesDirectory: args.resourcesDirectory,
		resources: args.resources,
		getResource: (name: string) => args.resources.find((resource) => resource.name === name),
		reload: () => Effect.void,
		updateModel: () => Effect.fail('not implemented'),
		addResource: () => Effect.fail('not implemented'),
		removeResource: () => Effect.fail('not implemented'),
		getProviderOptions: () => undefined,
		model: 'test-model',
		provider: 'test-provider',
		maxSteps: 10,
		configPath: args.configPath ?? path.join(args.resourcesDirectory, 'btca.config.jsonc')
	}) as unknown as ConfigService;

export const createStaleLockDirectory = async (lockPath: string) => {
	await fs.mkdir(lockPath, { recursive: true });
	const oldDate = new Date(Date.now() - 60_000);
	await fs.utimes(lockPath, oldDate, oldDate);
};
