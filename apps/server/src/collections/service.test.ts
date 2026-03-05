import { describe, expect, it } from 'bun:test';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import type { ConfigService } from '../config/index.ts';
import type { ResourceDefinition } from '../resources/schema.ts';
import type { ResourcesService } from '../resources/service.ts';
import type { BtcaFsResource } from '../resources/types.ts';
import { createCollectionsService } from './service.ts';
import { disposeVirtualFs, existsInVirtualFs } from '../vfs/virtual-fs.ts';

const createFsResource = ({
	name,
	resourcePath,
	type = 'local',
	repoSubPaths = [],
	specialAgentInstructions = ''
}: {
	name: string;
	resourcePath: string;
	type?: BtcaFsResource['type'];
	repoSubPaths?: readonly string[];
	specialAgentInstructions?: string;
}) => ({
	_tag: 'fs-based' as const,
	name,
	fsName: name,
	type,
	repoSubPaths,
	specialAgentInstructions,
	getAbsoluteDirectoryPath: async () => resourcePath
});

const createConfigMock = (definitions: Record<string, ResourceDefinition> = {}) =>
	({
		getResource: (name: string) => definitions[name]
	}) as unknown as ConfigService;

const createResourcesMock = (loadPromise: ResourcesService['loadPromise']) =>
	({
		load: () => {
			throw new Error('Not implemented in test');
		},
		loadPromise
	}) as unknown as ResourcesService;

const runGit = (cwd: string, args: string[]) => {
	const result = Bun.spawnSync({
		cmd: ['git', ...args],
		cwd,
		stdout: 'pipe',
		stderr: 'pipe'
	});
	if (result.exitCode !== 0) {
		throw new Error(
			`git ${args.join(' ')} failed: ${new TextDecoder().decode(result.stderr).trim()}`
		);
	}
};

const cleanupCollection = async (collection: { vfsId?: string; cleanup?: () => Promise<void> }) => {
	await collection.cleanup?.();
	if (collection.vfsId) disposeVirtualFs(collection.vfsId);
};

describe('createCollectionsService', () => {
	it('imports git-backed local resources from tracked and unignored files only', async () => {
		const resourcePath = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-collections-git-'));
		const collections = createCollectionsService({
			config: createConfigMock(),
			resources: createResourcesMock(async () => createFsResource({ name: 'repo', resourcePath }))
		});

		try {
			await fs.mkdir(path.join(resourcePath, 'node_modules', 'pkg'), { recursive: true });
			await fs.writeFile(path.join(resourcePath, '.gitignore'), 'node_modules\n');
			await fs.writeFile(path.join(resourcePath, 'package.json'), '{"name":"repo"}\n');
			await fs.writeFile(path.join(resourcePath, 'README.md'), 'local notes\n');
			await fs.writeFile(path.join(resourcePath, 'node_modules', 'pkg', 'index.js'), 'ignored\n');

			runGit(resourcePath, ['init', '-q']);
			runGit(resourcePath, ['add', '.gitignore', 'package.json']);

			const collection = await collections.loadPromise({ resourceNames: ['repo'] });

			try {
				expect(await existsInVirtualFs('/repo/package.json', collection.vfsId)).toBe(true);
				expect(await existsInVirtualFs('/repo/README.md', collection.vfsId)).toBe(true);
				expect(await existsInVirtualFs('/repo/node_modules/pkg/index.js', collection.vfsId)).toBe(
					false
				);
				expect(await existsInVirtualFs('/repo/.git/config', collection.vfsId)).toBe(false);
			} finally {
				await cleanupCollection(collection);
			}
		} finally {
			await fs.rm(resourcePath, { recursive: true, force: true });
		}
	});

	it('falls back to directory import and still skips heavy local build directories', async () => {
		const resourcePath = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-collections-local-'));
		const collections = createCollectionsService({
			config: createConfigMock(),
			resources: createResourcesMock(async () => createFsResource({ name: 'repo', resourcePath }))
		});

		try {
			await fs.mkdir(path.join(resourcePath, 'node_modules', 'pkg'), { recursive: true });
			await fs.mkdir(path.join(resourcePath, 'dist'), { recursive: true });
			await fs.writeFile(path.join(resourcePath, 'package.json'), '{"name":"repo"}\n');
			await fs.writeFile(path.join(resourcePath, 'README.md'), 'hello\n');
			await fs.writeFile(path.join(resourcePath, 'node_modules', 'pkg', 'index.js'), 'ignored\n');
			await fs.writeFile(path.join(resourcePath, 'dist', 'bundle.js'), 'ignored\n');

			const collection = await collections.loadPromise({ resourceNames: ['repo'] });

			try {
				expect(await existsInVirtualFs('/repo/package.json', collection.vfsId)).toBe(true);
				expect(await existsInVirtualFs('/repo/README.md', collection.vfsId)).toBe(true);
				expect(await existsInVirtualFs('/repo/node_modules/pkg/index.js', collection.vfsId)).toBe(
					false
				);
				expect(await existsInVirtualFs('/repo/dist/bundle.js', collection.vfsId)).toBe(false);
				expect(collection.agentInstructions).not.toContain('<special_notes>');
			} finally {
				await cleanupCollection(collection);
			}
		} finally {
			await fs.rm(resourcePath, { recursive: true, force: true });
		}
	});

	it('includes git citation metadata in agent instructions', async () => {
		const resourcePath = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-collections-git-meta-'));
		const collections = createCollectionsService({
			config: createConfigMock({
				docs: {
					type: 'git',
					name: 'docs',
					url: 'https://github.com/example/repo.git',
					branch: 'main',
					searchPath: 'guides',
					specialNotes: 'Prefer the guides folder.'
				}
			}),
			resources: createResourcesMock(async () =>
				createFsResource({
					name: 'docs',
					resourcePath,
					type: 'git',
					repoSubPaths: ['guides'],
					specialAgentInstructions: 'Prefer the guides folder.'
				})
			)
		});

		try {
			await fs.writeFile(path.join(resourcePath, 'README.md'), 'hello\n');
			runGit(resourcePath, ['init', '-q']);
			runGit(resourcePath, ['config', 'user.email', 'test@example.com']);
			runGit(resourcePath, ['config', 'user.name', 'BTCA Test']);
			runGit(resourcePath, ['add', 'README.md']);
			runGit(resourcePath, ['commit', '-m', 'init']);

			const collection = await collections.loadPromise({ resourceNames: ['docs'] });

			try {
				expect(collection.agentInstructions).toContain(
					'<repo_url>https://github.com/example/repo</repo_url>'
				);
				expect(collection.agentInstructions).toContain('<repo_branch>main</repo_branch>');
				expect(collection.agentInstructions).toContain(
					'<github_blob_prefix>https://github.com/example/repo/blob/main</github_blob_prefix>'
				);
				expect(collection.agentInstructions).toContain(
					'<citation_rule>Convert virtual paths under ./docs/ to repo-relative paths, then encode each path segment for GitHub URLs.</citation_rule>'
				);
				expect(collection.agentInstructions).toContain('<path>./docs/guides</path>');
				expect(collection.agentInstructions).toContain('<repo_commit>');
				expect(collection.agentInstructions).toContain(
					'<special_notes>Prefer the guides folder.</special_notes>'
				);
			} finally {
				await cleanupCollection(collection);
			}
		} finally {
			await fs.rm(resourcePath, { recursive: true, force: true });
		}
	});

	it('includes npm citation metadata in agent instructions', async () => {
		const resourcePath = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-collections-npm-meta-'));
		const collections = createCollectionsService({
			config: createConfigMock({
				react: {
					type: 'npm',
					name: 'react',
					package: 'react',
					version: '19.0.0',
					specialNotes: 'Use package docs.'
				}
			}),
			resources: createResourcesMock(async () =>
				createFsResource({
					name: 'react',
					resourcePath,
					type: 'npm',
					specialAgentInstructions: 'Use package docs.'
				})
			)
		});

		try {
			await fs.writeFile(
				path.join(resourcePath, '.btca-npm-meta.json'),
				JSON.stringify({
					packageName: 'react',
					resolvedVersion: '19.0.0',
					packageUrl: 'https://www.npmjs.com/package/react'
				})
			);
			await fs.writeFile(path.join(resourcePath, 'README.md'), 'react docs\n');

			const collection = await collections.loadPromise({ resourceNames: ['react'] });

			try {
				expect(collection.agentInstructions).toContain('<npm_package>react</npm_package>');
				expect(collection.agentInstructions).toContain('<npm_version>19.0.0</npm_version>');
				expect(collection.agentInstructions).toContain(
					'<npm_url>https://www.npmjs.com/package/react</npm_url>'
				);
				expect(collection.agentInstructions).toContain(
					'<npm_citation_alias>npm:react@19.0.0</npm_citation_alias>'
				);
				expect(collection.agentInstructions).toContain(
					'<npm_file_url_prefix>https://unpkg.com/react@19.0.0</npm_file_url_prefix>'
				);
				expect(collection.agentInstructions).toContain(
					'<citation_rule>In Sources, cite npm files using npm:react@19.0.0/&lt;file&gt; and link them to https://unpkg.com/react@19.0.0/&lt;file&gt;. Do not cite encoded virtual folder names.</citation_rule>'
				);
				expect(collection.agentInstructions).toContain(
					'<citation_example>https://unpkg.com/react@19.0.0/package.json</citation_example>'
				);
				expect(collection.agentInstructions).toContain(
					'<special_notes>Use package docs.</special_notes>'
				);
			} finally {
				await cleanupCollection(collection);
			}
		} finally {
			await fs.rm(resourcePath, { recursive: true, force: true });
		}
	});
});
