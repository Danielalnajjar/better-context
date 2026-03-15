export const FS_RESOURCE_SYSTEM_NOTE =
	'This is a btca resource - a searchable knowledge source the agent can reference.';

export type BtcaGitMaterializationMetadata = {
	readonly url?: string;
	readonly branch?: string;
	readonly commit?: string;
};

export type BtcaNpmMaterializationMetadata = {
	readonly package?: string;
	readonly version?: string;
	readonly url?: string;
};

export type BtcaLocalMaterializationMetadata = Record<never, never>;

export type BtcaResourceMaterializationMetadata =
	| BtcaGitMaterializationMetadata
	| BtcaNpmMaterializationMetadata
	| BtcaLocalMaterializationMetadata;

type BtcaResourceType = 'git' | 'local' | 'npm';

type BtcaMaterializationMetadataByType = {
	readonly git: BtcaGitMaterializationMetadata;
	readonly local: BtcaLocalMaterializationMetadata;
	readonly npm: BtcaNpmMaterializationMetadata;
};

export type BtcaResourceMaterializationResult<TType extends BtcaResourceType = BtcaResourceType> = {
	readonly metadata: BtcaMaterializationMetadataByType[TType];
};

type BtcaFsResourceBase<TType extends BtcaResourceType> = {
	readonly _tag: 'fs-based';
	readonly name: string;
	readonly fsName: string;
	readonly type: TType;
	readonly repoSubPaths: readonly string[];
	readonly specialAgentInstructions: string;
	readonly materializeIntoVirtualFs: (args: {
		readonly destinationPath: string;
		readonly vfsId: string;
	}) => Promise<BtcaResourceMaterializationResult<TType>>;
	readonly cleanup?: () => Promise<void>;
};

export type BtcaGitFsResource = BtcaFsResourceBase<'git'>;
export type BtcaLocalFsResource = BtcaFsResourceBase<'local'>;
export type BtcaNpmFsResource = BtcaFsResourceBase<'npm'>;

export type BtcaFsResource = BtcaGitFsResource | BtcaLocalFsResource | BtcaNpmFsResource;

export type BtcaGitResourceArgs = {
	readonly type: 'git';
	readonly name: string;
	readonly url: string;
	readonly branch: string;
	readonly repoSubPaths: readonly string[];
	readonly resourcesDirectoryPath: string;
	readonly specialAgentInstructions: string;
	readonly quiet: boolean;
	readonly ephemeral?: boolean;
	readonly localDirectoryKey?: string;
};

export type BtcaLocalResourceArgs = {
	readonly type: 'local';
	readonly name: string;
	readonly path: string;
	readonly specialAgentInstructions: string;
};

export type BtcaNpmResourceArgs = {
	readonly type: 'npm';
	readonly name: string;
	readonly package: string;
	readonly version?: string;
	readonly resourcesDirectoryPath: string;
	readonly specialAgentInstructions: string;
	readonly quiet: boolean;
	readonly ephemeral?: boolean;
	readonly localDirectoryKey?: string;
};
