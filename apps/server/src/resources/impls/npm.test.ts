import { afterEach, beforeEach, describe, expect, it, jest } from 'bun:test';
import { promises as fs } from 'node:fs';
import { mkdirSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { loadNpmResource, type NpmResourceDeps } from './npm.ts';
import type { BtcaNpmResourceArgs } from '../types.ts';
import { createVirtualFs, disposeVirtualFs } from '../../vfs/virtual-fs.ts';

const streamFromString = (value: string) =>
	new ReadableStream<Uint8Array>({
		start(controller) {
			if (value.length > 0) controller.enqueue(new TextEncoder().encode(value));
			controller.close();
		}
	});

const parsePackageSpec = (value: string) => {
	const splitIndex = value.lastIndexOf('@');
	if (splitIndex <= 0 || splitIndex === value.length - 1) {
		return { packageName: value, version: '0.0.0' };
	}
	return {
		packageName: value.slice(0, splitIndex),
		version: value.slice(splitIndex + 1)
	};
};

const createInstallSpawnMock = (args?: { exitCode?: number; stdout?: string; stderr?: string }) =>
	((...spawnArgs: Parameters<typeof Bun.spawn>) => {
		const [command, options] = spawnArgs;
		const commandArgs = Array.isArray(command) ? command : [command];
		const packageSpec = commandArgs.at(-1) ?? '';
		const { packageName, version } = parsePackageSpec(packageSpec);
		const cwd = options?.cwd;

		if ((args?.exitCode ?? 0) === 0 && cwd) {
			const packageDirectory = path.join(cwd, 'node_modules', ...packageName.split('/'));
			mkdirSync(path.join(packageDirectory, 'src'), { recursive: true });
			const title = packageName === 'react' ? 'React' : packageName;
			writeFileSync(path.join(packageDirectory, 'README.md'), `# ${title}\n\nInstalled for btca`);
			writeFileSync(
				path.join(packageDirectory, 'package.json'),
				JSON.stringify({ name: packageName, version }, null, 2)
			);
			writeFileSync(
				path.join(packageDirectory, 'src', 'runtime.js'),
				`export const rune = '$state';`
			);
		}

		return {
			stdout: streamFromString(args?.stdout ?? ''),
			stderr: streamFromString(args?.stderr ?? ''),
			exited: Promise.resolve(args?.exitCode ?? 0)
		} as unknown as ReturnType<typeof Bun.spawn>;
	}) as typeof Bun.spawn;

const createNpmTestDeps = (spawn: NpmResourceDeps['spawn']): Partial<NpmResourceDeps> => ({
	spawn
});

const materializeResource = async (
	resource: Awaited<ReturnType<typeof loadNpmResource>>,
	destinationPath = '/resource'
) => {
	const vfsId = createVirtualFs();
	try {
		const result = await resource.materializeIntoVirtualFs({ destinationPath, vfsId });
		return { result, vfsId };
	} catch (cause) {
		disposeVirtualFs(vfsId);
		throw cause;
	}
};

describe('NPM Resource', () => {
	let testDir: string;
	let originalFetch: typeof fetch;

	beforeEach(async () => {
		testDir = await fs.mkdtemp(path.join(os.tmpdir(), 'btca-npm-test-'));
		originalFetch = globalThis.fetch;
	});

	afterEach(async () => {
		globalThis.fetch = originalFetch;
		await fs.rm(testDir, { recursive: true, force: true });
	});

	it('hydrates an npm package into a filesystem resource', async () => {
		const npmDeps = createNpmTestDeps(createInstallSpawnMock());
		globalThis.fetch = (async (input) => {
			const url = String(input);
			if (url.startsWith('https://registry.npmjs.org/react')) {
				return new Response(
					JSON.stringify({
						'dist-tags': { latest: '19.0.0' },
						versions: {
							'19.0.0': {
								name: 'react',
								version: '19.0.0',
								description: 'React',
								readme: '# React\n\nDocs'
							}
						}
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } }
				);
			}
			if (url.startsWith('https://www.npmjs.com/package/react/v/19.0.0')) {
				return new Response('<html><title>react</title></html>', { status: 200 });
			}
			return new Response('not found', { status: 404 });
		}) as typeof fetch;

		const args: BtcaNpmResourceArgs = {
			type: 'npm',
			name: 'react-docs',
			package: 'react',
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: 'Use this for React questions',
			quiet: true
		};

		const resource = await loadNpmResource(args, npmDeps);
		expect(resource._tag).toBe('fs-based');
		expect(resource.type).toBe('npm');
		expect(resource.repoSubPaths).toEqual([]);

		const { result, vfsId } = await materializeResource(resource, '/react-docs');
		const resourcePath = path.join(testDir, 'react-docs');
		expect(result.metadata.package).toBe('react');

		try {
			const readme = await Bun.file(path.join(resourcePath, 'README.md')).text();
			expect(readme).toContain('# React');
			const runtimeFile = await Bun.file(path.join(resourcePath, 'src', 'runtime.js')).text();
			expect(runtimeFile).toContain(`'$state'`);

			const packagePage = await Bun.file(path.join(resourcePath, 'npm-package-page.html')).text();
			expect(packagePage).toContain('<title>react</title>');
		} finally {
			disposeVirtualFs(vfsId);
		}
	});

	it('uses the resolved installed version for tag-based npm metadata', async () => {
		const npmDeps = createNpmTestDeps(createInstallSpawnMock());
		globalThis.fetch = (async (input) => {
			const url = String(input);
			if (url.startsWith('https://registry.npmjs.org/react')) {
				return new Response(
					JSON.stringify({
						'dist-tags': { latest: '19.1.0' },
						versions: {
							'19.1.0': {
								name: 'react',
								version: '19.1.0',
								description: 'React',
								readme: '# React\n\nDocs'
							}
						}
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } }
				);
			}
			if (url.startsWith('https://www.npmjs.com/package/react/v/19.1.0')) {
				return new Response('<html><title>react</title></html>', { status: 200 });
			}
			return new Response('not found', { status: 404 });
		}) as typeof fetch;

		const resource = await loadNpmResource(
			{
				type: 'npm',
				name: 'react-latest',
				package: 'react',
				version: 'latest',
				resourcesDirectoryPath: testDir,
				specialAgentInstructions: '',
				quiet: true
			},
			npmDeps
		);

		const { result, vfsId } = await materializeResource(resource, '/react-latest');
		try {
			expect(result.metadata.version).toBe('19.1.0');

			const meta = JSON.parse(
				await Bun.file(path.join(testDir, 'react-latest', '.btca-npm-meta.json')).text()
			) as { requestedVersion?: string; resolvedVersion: string };
			expect(meta.requestedVersion).toBe('latest');
			expect(meta.resolvedVersion).toBe('19.1.0');
		} finally {
			disposeVirtualFs(vfsId);
		}
	});

	it('reuses cached pinned versions without refetching', async () => {
		let fetchCalls = 0;
		let spawnCalls = 0;
		const installSpawnMock = createInstallSpawnMock();
		const npmDeps = createNpmTestDeps(((...spawnArgs: Parameters<typeof Bun.spawn>) => {
			spawnCalls += 1;
			return installSpawnMock(...spawnArgs);
		}) as typeof Bun.spawn);
		globalThis.fetch = (async (input) => {
			fetchCalls += 1;
			const url = String(input);
			if (url.startsWith('https://registry.npmjs.org/%40types%2Fnode')) {
				return new Response(
					JSON.stringify({
						'dist-tags': { latest: '22.10.1' },
						versions: {
							'22.10.1': {
								name: '@types/node',
								version: '22.10.1',
								readme: '# @types/node'
							}
						}
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } }
				);
			}
			if (url.startsWith('https://www.npmjs.com/package/%40types/node/v/22.10.1')) {
				return new Response('<html><title>@types/node</title></html>', { status: 200 });
			}
			return new Response('not found', { status: 404 });
		}) as typeof fetch;

		const args: BtcaNpmResourceArgs = {
			type: 'npm',
			name: 'node-types',
			package: '@types/node',
			version: '22.10.1',
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: '',
			quiet: true
		};

		const first = await loadNpmResource(args, npmDeps);
		const firstMaterialized = await materializeResource(first, '/node-types-a');
		disposeVirtualFs(firstMaterialized.vfsId);
		const firstFetchCalls = fetchCalls;
		const firstSpawnCalls = spawnCalls;
		const second = await loadNpmResource(args, npmDeps);
		const secondMaterialized = await materializeResource(second, '/node-types-b');
		disposeVirtualFs(secondMaterialized.vfsId);
		expect(fetchCalls).toBe(firstFetchCalls);
		expect(spawnCalls).toBe(firstSpawnCalls);
	});

	it('adds cleanup for anonymous npm resources', async () => {
		const npmDeps = createNpmTestDeps(createInstallSpawnMock());
		globalThis.fetch = (async (input) => {
			const url = String(input);
			if (url.startsWith('https://registry.npmjs.org/react')) {
				return new Response(
					JSON.stringify({
						'dist-tags': { latest: '19.0.0' },
						versions: {
							'19.0.0': { name: 'react', version: '19.0.0', readme: '# React' }
						}
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } }
				);
			}
			if (url.startsWith('https://www.npmjs.com/package/react/v/19.0.0')) {
				return new Response('<html><title>react</title></html>', { status: 200 });
			}
			return new Response('not found', { status: 404 });
		}) as typeof fetch;

		const args: BtcaNpmResourceArgs = {
			type: 'npm',
			name: 'anonymous:npm:react',
			package: 'react',
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: '',
			quiet: true,
			ephemeral: true,
			localDirectoryKey: 'anonymous-react'
		};

		const resource = await loadNpmResource(args, npmDeps);
		expect(resource.cleanup).toBeDefined();

		const materialized = await materializeResource(resource, '/anonymous-react');
		disposeVirtualFs(materialized.vfsId);
		const resourcePath = path.join(testDir, '.tmp', 'anonymous-react');
		let existsBefore = false;
		try {
			await fs.stat(resourcePath);
			existsBefore = true;
		} catch {
			existsBefore = false;
		}
		expect(existsBefore).toBe(true);

		await resource.cleanup?.();
		let existsAfter = false;
		try {
			await fs.stat(resourcePath);
			existsAfter = true;
		} catch {
			existsAfter = false;
		}
		expect(existsAfter).toBe(false);
	});

	it('uses readable fsName aliases for anonymous npm resources', async () => {
		const npmDeps = createNpmTestDeps(createInstallSpawnMock());
		globalThis.fetch = (async (input) => {
			const url = String(input);
			if (url.startsWith('https://registry.npmjs.org/%40types%2Fnode')) {
				return new Response(
					JSON.stringify({
						'dist-tags': { latest: '22.10.1' },
						versions: {
							'22.10.1': { name: '@types/node', version: '22.10.1', readme: '# @types/node' }
						}
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } }
				);
			}
			if (url.startsWith('https://www.npmjs.com/package/%40types/node/v/22.10.1')) {
				return new Response('<html><title>@types/node</title></html>', { status: 200 });
			}
			return new Response('not found', { status: 404 });
		}) as typeof fetch;

		const args: BtcaNpmResourceArgs = {
			type: 'npm',
			name: 'anonymous:npm:@types/node@22.10.1',
			package: '@types/node',
			version: '22.10.1',
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: '',
			quiet: true,
			ephemeral: true,
			localDirectoryKey: 'anonymous-types-node'
		};

		const resource = await loadNpmResource(args, npmDeps);
		expect(resource.fsName).toBe('npm:@types__node@22.10.1--anonymous-types-node');
		expect(resource.fsName.includes('%3A')).toBe(false);
		expect(resource.fsName.includes('%2F')).toBe(false);
	});

	it('keeps anonymous npm fsName values collision-free for distinct packages with similar sanitized names', async () => {
		const first = await loadNpmResource({
			type: 'npm',
			name: 'anonymous:npm:@foo/bar__baz@1.0.0',
			package: '@foo/bar__baz',
			version: '1.0.0',
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: '',
			quiet: true,
			ephemeral: true,
			localDirectoryKey: 'anonymous-first'
		});
		const second = await loadNpmResource({
			type: 'npm',
			name: 'anonymous:npm:@foo__bar/baz@1.0.0',
			package: '@foo__bar/baz',
			version: '1.0.0',
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: '',
			quiet: true,
			ephemeral: true,
			localDirectoryKey: 'anonymous-second'
		});

		expect(first.fsName).not.toBe(second.fsName);
		expect(first.fsName).toContain('anonymous-first');
		expect(second.fsName).toContain('anonymous-second');
	});

	it('continues when npm package page fetch is unavailable', async () => {
		const npmDeps = createNpmTestDeps(createInstallSpawnMock());
		globalThis.fetch = (async (input) => {
			const url = String(input);
			if (url.startsWith('https://registry.npmjs.org/react')) {
				return new Response(
					JSON.stringify({
						'dist-tags': { latest: '19.0.0' },
						versions: {
							'19.0.0': {
								name: 'react',
								version: '19.0.0',
								description: 'React'
							}
						}
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } }
				);
			}
			if (url.startsWith('https://www.npmjs.com/package/react/v/19.0.0')) {
				return new Response('blocked', { status: 403 });
			}
			return new Response('not found', { status: 404 });
		}) as typeof fetch;

		const args: BtcaNpmResourceArgs = {
			type: 'npm',
			name: 'react-docs',
			package: 'react',
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: '',
			quiet: true
		};

		const resource = await loadNpmResource(args, npmDeps);
		const { vfsId } = await materializeResource(resource, '/react-docs-fallback');
		try {
			const resourcePath = path.join(testDir, 'react-docs');
			const packagePage = await Bun.file(path.join(resourcePath, 'npm-package-page.html')).text();
			expect(packagePage).toContain('npm package page unavailable');
		} finally {
			disposeVirtualFs(vfsId);
		}
	});

	it('returns a clear install error when bun install fails', async () => {
		const npmDeps = createNpmTestDeps(
			createInstallSpawnMock({
				exitCode: 1,
				stderr: 'error: package not found'
			})
		);
		globalThis.fetch = (async (input) => {
			const url = String(input);
			if (url.startsWith('https://registry.npmjs.org/react')) {
				return new Response(
					JSON.stringify({
						'dist-tags': { latest: '19.0.0' },
						versions: {
							'19.0.0': {
								name: 'react',
								version: '19.0.0',
								description: 'React'
							}
						}
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } }
				);
			}
			if (url.startsWith('https://www.npmjs.com/package/react/v/19.0.0')) {
				return new Response('<html><title>react</title></html>', { status: 200 });
			}
			return new Response('not found', { status: 404 });
		}) as typeof fetch;

		const args: BtcaNpmResourceArgs = {
			type: 'npm',
			name: 'react-docs',
			package: 'react',
			resourcesDirectoryPath: testDir,
			specialAgentInstructions: '',
			quiet: true
		};

		const resource = await loadNpmResource(args, npmDeps);
		await expect(materializeResource(resource, '/react-docs-error')).rejects.toThrow(
			'Failed to install npm package "react@19.0.0"'
		);
	});

	it('does not publish cache metadata when payload writes fail', async () => {
		const npmDeps = createNpmTestDeps(createInstallSpawnMock());
		globalThis.fetch = (async (input) => {
			const url = String(input);
			if (url.startsWith('https://registry.npmjs.org/react')) {
				return new Response(
					JSON.stringify({
						'dist-tags': { latest: '19.0.0' },
						versions: {
							'19.0.0': {
								name: 'react',
								version: '19.0.0',
								description: 'React',
								readme: '# React\n\nDocs'
							}
						}
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } }
				);
			}
			if (url.startsWith('https://www.npmjs.com/package/react/v/19.0.0')) {
				return new Response('<html><title>react</title></html>', { status: 200 });
			}
			return new Response('not found', { status: 404 });
		}) as typeof fetch;

		const originalWrite = Bun.write.bind(Bun);
		const writeSpy = jest.spyOn(Bun, 'write').mockImplementation(async (...writeArgs) => {
			const target = writeArgs[0];
			if (
				typeof target === 'string' &&
				target.endsWith(path.join('react-docs', 'npm-package-page.html'))
			) {
				throw new Error('page write failed');
			}
			return originalWrite(
				writeArgs[0] as Parameters<typeof Bun.write>[0],
				writeArgs[1] as Parameters<typeof Bun.write>[1],
				writeArgs[2] as Parameters<typeof Bun.write>[2]
			);
		});

		try {
			const resource = await loadNpmResource(
				{
					type: 'npm',
					name: 'react-docs',
					package: 'react',
					resourcesDirectoryPath: testDir,
					specialAgentInstructions: '',
					quiet: true
				},
				npmDeps
			);

			await expect(materializeResource(resource, '/react-docs-error')).rejects.toThrow(
				'page write failed'
			);
		} finally {
			writeSpy.mockRestore();
		}

		expect(await Bun.file(path.join(testDir, 'react-docs', '.btca-npm-meta.json')).exists()).toBe(
			false
		);
	});
});
