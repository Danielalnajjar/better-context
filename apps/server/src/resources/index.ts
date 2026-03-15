export { ResourceError } from './helpers.ts';
export {
	createResourcesService,
	createAnonymousResource,
	resolveResourceDefinition
} from './service.ts';
export type { ResourcesService } from './service.ts';
export {
	GitResourceSchema,
	LocalResourceSchema,
	NpmResourceSchema,
	ResourceDefinitionSchema,
	isGitResource,
	isLocalResource,
	isNpmResource,
	type GitResource,
	type LocalResource,
	type NpmResource,
	type ResourceDefinition
} from './schema.ts';
export {
	FS_RESOURCE_SYSTEM_NOTE,
	type BtcaFsResource,
	type BtcaGitFsResource,
	type BtcaGitResourceArgs,
	type BtcaGitMaterializationMetadata,
	type BtcaLocalFsResource,
	type BtcaLocalMaterializationMetadata,
	type BtcaNpmFsResource,
	type BtcaNpmMaterializationMetadata,
	type BtcaResourceMaterializationMetadata,
	type BtcaResourceMaterializationResult,
	type BtcaNpmResourceArgs
} from './types.ts';
