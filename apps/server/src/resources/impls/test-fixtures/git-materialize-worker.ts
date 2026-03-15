import { setTimeout as sleep } from 'node:timers/promises';

import { disposeVirtualFs, createVirtualFs } from '../../../vfs/virtual-fs.ts';
import { loadGitResource } from '../git.ts';

type WorkerPayload = {
	readonly gitBinDir?: string;
	readonly resourcesDirectory: string;
	readonly name: string;
	readonly url: string;
	readonly branch: string;
	readonly repoSubPaths?: readonly string[];
	readonly ephemeral?: boolean;
	readonly localDirectoryKey?: string;
	readonly quiet?: boolean;
	readonly cleanupAfter?: boolean;
	readonly pauseAfterMs?: number;
};

const payloadText = process.env.BTCA_WORKER_PAYLOAD;
if (!payloadText) {
	console.error('BTCA_WORKER_PAYLOAD is required');
	process.exit(1);
}

const payload = JSON.parse(payloadText) as WorkerPayload;
if (payload.gitBinDir) {
	process.env.PATH = `${payload.gitBinDir}:${process.env.PATH ?? ''}`;
}

const resource = await loadGitResource({
	type: 'git',
	name: payload.name,
	url: payload.url,
	branch: payload.branch,
	repoSubPaths: payload.repoSubPaths ?? [],
	resourcesDirectoryPath: payload.resourcesDirectory,
	specialAgentInstructions: '',
	quiet: payload.quiet ?? true,
	...(payload.localDirectoryKey ? { localDirectoryKey: payload.localDirectoryKey } : {}),
	...(payload.ephemeral ? { ephemeral: true } : {})
});

const vfsId = createVirtualFs();
try {
	const result = await resource.materializeIntoVirtualFs({
		destinationPath: '/resource',
		vfsId
	});
	if (payload.pauseAfterMs) {
		await sleep(payload.pauseAfterMs);
	}
	if (payload.cleanupAfter) {
		await resource.cleanup?.();
	}
	console.log(JSON.stringify(result));
} finally {
	disposeVirtualFs(vfsId);
}
