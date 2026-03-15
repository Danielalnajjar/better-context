export type VirtualResourceMetadata = {
	name: string;
	fsName: string;
	type: 'git' | 'local' | 'npm';
	repoSubPaths: readonly string[];
	url?: string;
	branch?: string;
	commit?: string;
	package?: string;
	version?: string;
	loadedAt: string;
};

export type VirtualCollectionMetadata = {
	vfsId: string;
	collectionKey: string;
	createdAt: string;
	resources: VirtualResourceMetadata[];
};

const metadataByVfsId = new Map<string, VirtualCollectionMetadata>();

export const setVirtualCollectionMetadata = (metadata: VirtualCollectionMetadata) => {
	metadataByVfsId.set(metadata.vfsId, metadata);
};

export const getVirtualCollectionMetadata = (vfsId: string) => metadataByVfsId.get(vfsId);

export const clearVirtualCollectionMetadata = (vfsId: string) => metadataByVfsId.delete(vfsId);

export const clearAllVirtualCollectionMetadata = () => {
	metadataByVfsId.clear();
};
