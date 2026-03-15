import { Layer, ServiceMap, pipe } from 'effect';
import type { AgentService as AgentServiceShape } from '../agent/service.ts';
import type { CollectionsService as CollectionsServiceShape } from '../collections/service.ts';
import type { ConfigService as ConfigServiceShape } from '../config/index.ts';
import type { ResourcesService as ResourcesServiceShape } from '../resources/service.ts';
import { AgentService, CollectionsService, ConfigService, ResourcesService } from './services.ts';

export type ServerLayerDependencies = {
	config: ConfigServiceShape;
	collections: CollectionsServiceShape;
	agent: AgentServiceShape;
	resources: ResourcesServiceShape;
};

export const makeServerLayer = (dependencies: ServerLayerDependencies) =>
	Layer.mergeAll(
		Layer.succeed(ConfigService, dependencies.config),
		Layer.succeed(CollectionsService, dependencies.collections),
		Layer.succeed(AgentService, dependencies.agent),
		Layer.succeed(ResourcesService, dependencies.resources)
	);

export const makeServerServiceMap = (dependencies: ServerLayerDependencies) =>
	pipe(
		ServiceMap.empty(),
		ServiceMap.add(ConfigService, dependencies.config),
		ServiceMap.add(CollectionsService, dependencies.collections),
		ServiceMap.add(AgentService, dependencies.agent),
		ServiceMap.add(ResourcesService, dependencies.resources)
	);
