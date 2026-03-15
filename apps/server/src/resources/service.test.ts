import { describe, expect, it } from 'bun:test';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
	createAnonymousDirectoryKey,
	createAnonymousResource,
	createResourcesService,
	resolveResourceDefinition
} from './service.ts';
import { resourceNameToKey } from './helpers.ts';
import { getClearTrashRoot } from './layout.ts';
import { type ResourceDefinition } from './schema.ts';
import { createConfigServiceMock, createStaleLockDirectory } from './test-support.ts';

describe('Resources.resolveResourceDefinition', () => {
	const configuredResource: ResourceDefinition = {
		type: 'git',
		name: 'svelte',
		url: 'https://github.com/sveltejs/svelte.dev',
		branch: 'main',
		searchPath: 'apps/svelte.dev'
	};

	const getResource = (name: string) => (name === 'svelte' ? configuredResource : undefined);

	it('resolves configured resources by name first', () => {
		const definition = resolveResourceDefinition('svelte', getResource);
		expect(definition.type).toBe('git');
		expect(definition.name).toBe('svelte');
	});

	it('creates anonymous git resources from valid URLs', () => {
		const definition = resolveResourceDefinition(
			'https://github.com/sveltejs/svelte.dev/tree/main/packages',
			() => undefined
		);
		expect(definition.type).toBe('git');
		if (definition.type === 'git') {
			expect(definition.url).toBe('https://github.com/sveltejs/svelte.dev');
			expect(definition.branch).toBe('main');
			expect(definition.name.startsWith('anonymous:')).toBe(true);
		}
	});

	it('creates anonymous npm resources from npm references', () => {
		const definition = resolveResourceDefinition('npm:@types/node@22.10.1', () => undefined);
		expect(definition.type).toBe('npm');
		if (definition.type === 'npm') {
			expect(definition.package).toBe('@types/node');
			expect(definition.version).toBe('22.10.1');
			expect(definition.name).toBe('anonymous:npm:@types/node@22.10.1');
		}
	});

	it('creates anonymous npm resources from npm package URLs', () => {
		const definition = resolveResourceDefinition(
			'https://www.npmjs.com/package/react/v/19.0.0',
			() => undefined
		);
		expect(definition.type).toBe('npm');
		if (definition.type === 'npm') {
			expect(definition.package).toBe('react');
			expect(definition.version).toBe('19.0.0');
		}
	});

	it('reuses the same cache key for repeated normalized URLs', () => {
		const first = createAnonymousResource('https://github.com/sveltejs/svelte.dev');
		const second = createAnonymousResource(
			'https://github.com/sveltejs/svelte.dev/blob/main/packages'
		);
		expect(first).not.toBeNull();
		expect(second).not.toBeNull();
		if (first && second) {
			expect(resourceNameToKey(first.name)).toBe(resourceNameToKey(second.name));
		}
	});

	it('uses short deterministic keys for anonymous repository paths', () => {
		const main = createAnonymousResource('https://github.com/sveltejs/svelte.dev');
		const withPath = createAnonymousResource(
			'https://github.com/sveltejs/svelte.dev/tree/main/packages'
		);
		expect(main).not.toBeNull();
		expect(withPath).not.toBeNull();
		if (main && withPath && main.type === 'git' && withPath.type === 'git') {
			expect(createAnonymousDirectoryKey(main.url)).toBe(createAnonymousDirectoryKey(withPath.url));
		}
		if (main) {
			expect(main.name.startsWith('anonymous:')).toBe(true);
			expect(main.name.length).toBeGreaterThan(19);
		}
	});
});

describe('Resources.clearCachesPromise', () => {
	it('drains opposite namespaces safely when git and npm reuse the same bare key', async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-clear-test-'));
		try {
			const resourcesDirectory = path.join(tempDir, 'resources');
			await fs.mkdir(path.join(resourcesDirectory, '.git-mirrors', 'foo', 'repo', '.git'), {
				recursive: true
			});
			await fs.mkdir(path.join(resourcesDirectory, 'bar'), { recursive: true });
			await fs.mkdir(path.join(resourcesDirectory, 'foo'), { recursive: true });
			await Bun.write(path.join(resourcesDirectory, 'bar', '.btca-npm-meta.json'), '{}');
			await Bun.write(path.join(resourcesDirectory, 'foo', '.btca-npm-meta.json'), '{}');
			await createStaleLockDirectory(
				path.join(resourcesDirectory, '.resource-locks', 'npm.foo.lock')
			);

			const service = createResourcesService(
				createConfigServiceMock({
					resourcesDirectory,
					resources: [
						{
							type: 'git',
							name: 'foo',
							url: 'https://github.com/example/foo',
							branch: 'main'
						},
						{
							type: 'npm',
							name: 'bar',
							package: 'bar'
						}
					]
				})
			);

			const result = await service.clearCachesPromise();
			expect(result.cleared).toBe(3);
			expect(
				await fs
					.stat(path.join(resourcesDirectory, '.git-mirrors', 'foo'))
					.then(() => true)
					.catch(() => false)
			).toBe(false);
			expect(
				await fs
					.stat(path.join(resourcesDirectory, 'bar'))
					.then(() => true)
					.catch(() => false)
			).toBe(false);
			expect(
				await fs
					.stat(path.join(resourcesDirectory, 'foo'))
					.then(() => true)
					.catch(() => false)
			).toBe(false);
			expect(
				await fs
					.stat(path.join(resourcesDirectory, '.resource-locks'))
					.then((value) => value.isDirectory())
					.catch(() => false)
			).toBe(true);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true });
		}
	});

	it('classifies passive leftovers across mirror, top-level, tmp, and unknown caches', async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-clear-leftovers-'));
		try {
			const resourcesDirectory = path.join(tempDir, 'resources');
			await fs.mkdir(path.join(resourcesDirectory, '.git-mirrors', 'legacy-git', 'repo', '.git'), {
				recursive: true
			});
			await fs.mkdir(path.join(resourcesDirectory, 'legacy-npm'), { recursive: true });
			await fs.mkdir(path.join(resourcesDirectory, 'legacy-git-top', '.git'), { recursive: true });
			await fs.mkdir(path.join(resourcesDirectory, 'legacy-unknown'), { recursive: true });
			await fs.mkdir(path.join(resourcesDirectory, '.tmp', 'legacy-anon'), { recursive: true });
			await Bun.write(path.join(resourcesDirectory, 'legacy-npm', '.btca-npm-meta.json'), '{}');
			await Bun.write(
				path.join(resourcesDirectory, '.tmp', 'legacy-anon', '.btca-npm-meta.json'),
				'{}'
			);

			const service = createResourcesService(
				createConfigServiceMock({
					resourcesDirectory,
					resources: []
				})
			);

			const result = await service.clearCachesPromise();
			expect(result.cleared).toBe(5);
			expect(
				await fs
					.stat(path.join(resourcesDirectory, '.git-mirrors', 'legacy-git'))
					.then(() => true)
					.catch(() => false)
			).toBe(false);
			expect(
				await fs
					.stat(path.join(resourcesDirectory, 'legacy-npm'))
					.then(() => true)
					.catch(() => false)
			).toBe(false);
			expect(
				await fs
					.stat(path.join(resourcesDirectory, 'legacy-git-top'))
					.then(() => true)
					.catch(() => false)
			).toBe(false);
			expect(
				await fs
					.stat(path.join(resourcesDirectory, 'legacy-unknown'))
					.then(() => true)
					.catch(() => false)
			).toBe(false);
			expect(
				await fs
					.stat(path.join(resourcesDirectory, '.tmp', 'legacy-anon'))
					.then(() => true)
					.catch(() => false)
			).toBe(false);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true });
		}
	});

	it('clears leftover anonymous git temp directories under the git namespace', async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-clear-anon-git-'));
		try {
			const resourcesDirectory = path.join(tempDir, 'resources');
			const anonymousKey = createAnonymousDirectoryKey('https://example.com/repo.git');
			const firstTmpDir = path.join(
				resourcesDirectory,
				'.tmp',
				`btca-anon-git-${anonymousKey}-first`
			);
			const secondTmpDir = path.join(
				resourcesDirectory,
				'.tmp',
				`btca-anon-git-${anonymousKey}-second`
			);
			await fs.mkdir(firstTmpDir, { recursive: true });
			await fs.mkdir(secondTmpDir, { recursive: true });
			await Bun.write(path.join(firstTmpDir, 'README.md'), 'first');
			await Bun.write(path.join(secondTmpDir, 'README.md'), 'second');

			const service = createResourcesService(
				createConfigServiceMock({
					resourcesDirectory,
					resources: []
				})
			);

			const result = await service.clearCachesPromise();
			expect(result.cleared).toBe(2);
			expect(
				await fs
					.stat(firstTmpDir)
					.then(() => true)
					.catch(() => false)
			).toBe(false);
			expect(
				await fs
					.stat(secondTmpDir)
					.then(() => true)
					.catch(() => false)
			).toBe(false);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true });
		}
	});

	it('removes leftover clear-trash directories without treating them as legacy caches', async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-clear-trash-'));
		try {
			const resourcesDirectory = path.join(tempDir, 'resources');
			const trashRoot = getClearTrashRoot(resourcesDirectory);
			const orphanedTrashDir = path.join(trashRoot, 'legacy-trash');
			await fs.mkdir(orphanedTrashDir, { recursive: true });
			await Bun.write(path.join(orphanedTrashDir, 'README.md'), 'leftover');

			const service = createResourcesService(
				createConfigServiceMock({
					resourcesDirectory,
					resources: []
				})
			);

			const result = await service.clearCachesPromise();
			expect(result.cleared).toBe(0);
			expect(
				await fs
					.stat(orphanedTrashDir)
					.then(() => true)
					.catch(() => false)
			).toBe(false);
			expect(
				await fs
					.stat(trashRoot)
					.then((value) => value.isDirectory())
					.catch(() => false)
			).toBe(true);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true });
		}
	});

	it('ignores malformed active lock directory names while clearing caches', async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-clear-malformed-locks-'));
		try {
			const resourcesDirectory = path.join(tempDir, 'resources');
			const siblingDirectory = path.join(tempDir, 'outside');
			await fs.mkdir(resourcesDirectory, { recursive: true });
			await fs.mkdir(siblingDirectory, { recursive: true });
			await Bun.write(path.join(siblingDirectory, 'keep.txt'), 'keep');
			await fs.mkdir(path.join(resourcesDirectory, '.resource-locks', 'npm..lock'), {
				recursive: true
			});
			await fs.mkdir(path.join(resourcesDirectory, '.resource-locks', 'npm....lock'), {
				recursive: true
			});
			await fs.mkdir(path.join(resourcesDirectory, '.resource-locks', 'git.foo bar.lock'), {
				recursive: true
			});

			const service = createResourcesService(
				createConfigServiceMock({
					resourcesDirectory,
					resources: []
				})
			);

			const result = await service.clearCachesPromise();
			expect(result.cleared).toBe(0);
			expect(
				await fs
					.stat(resourcesDirectory)
					.then((value) => value.isDirectory())
					.catch(() => false)
			).toBe(true);
			expect(
				await fs
					.stat(path.join(siblingDirectory, 'keep.txt'))
					.then(() => true)
					.catch(() => false)
			).toBe(true);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true });
		}
	});
});
