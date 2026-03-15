import { importDirectoryIntoVirtualFs, importPathsIntoVirtualFs } from '../../vfs/virtual-fs.ts';
import { type BtcaLocalFsResource, type BtcaLocalResourceArgs } from '../types.ts';
import { ResourceError, resourceNameToKey, shouldIgnoreLocalImportedPath } from '../helpers.ts';

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

export const loadLocalResource = (args: BtcaLocalResourceArgs): BtcaLocalFsResource => ({
	_tag: 'fs-based',
	name: args.name,
	fsName: resourceNameToKey(args.name),
	type: 'local',
	repoSubPaths: [],
	specialAgentInstructions: args.specialAgentInstructions,
	materializeIntoVirtualFs: async ({ destinationPath, vfsId }) => {
		try {
			const gitVisiblePaths = await listGitVisiblePaths(args.path);
			if (gitVisiblePaths) {
				await importPathsIntoVirtualFs({
					sourcePath: args.path,
					destinationPath,
					relativePaths: gitVisiblePaths,
					vfsId
				});
			} else {
				await importDirectoryIntoVirtualFs({
					sourcePath: args.path,
					destinationPath,
					vfsId,
					ignore: shouldIgnoreLocalImportedPath
				});
			}
		} catch (cause) {
			throw new ResourceError({
				message: `Failed to materialize local resource "${args.name}"`,
				hint: 'Check that the local path exists and is readable.',
				cause
			});
		}

		return { metadata: {} };
	}
});
