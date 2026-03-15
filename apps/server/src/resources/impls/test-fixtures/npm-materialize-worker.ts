import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

import type { NpmResourceDeps } from '../npm.ts';
import { setFilesystemLockTestHookForTests, type FilesystemLockTestEvent } from '../../lock.ts';
import { createResourcesService } from '../../service.ts';
import type { ResourceDefinition } from '../../schema.ts';
import { createConfigServiceMock } from '../../test-support.ts';
import { createVirtualFs, disposeVirtualFs } from '../../../vfs/virtual-fs.ts';

type WorkerAction =
	| {
			readonly type: 'materialize';
			readonly reference: string;
			readonly destinationPath?: string;
	  }
	| {
			readonly type: 'cleanup';
			readonly reference: string;
	  }
	| {
			readonly type: 'clear';
	  };

type WorkerMockConfig = {
	readonly packageName: string;
	readonly resolvedVersion: string;
	readonly pageHtml?: string;
	readonly holdInstallUntilContinue?: boolean;
	readonly exitCode?: number;
	readonly stdout?: string;
	readonly stderr?: string;
	readonly exitBeforeResult?: boolean;
};

type WorkerConfig = {
	readonly resourcesDirectory: string;
	readonly resources: readonly ResourceDefinition[];
	readonly action: WorkerAction;
	readonly mock: WorkerMockConfig;
};

type ChildToParentMessage =
	| {
			readonly type: 'install-started';
			readonly packageSpec: string;
			readonly cwd?: string;
	  }
	| {
			readonly type: 'lock-event';
			readonly event: FilesystemLockTestEvent;
	  }
	| {
			readonly type: 'result';
			readonly result: unknown;
	  }
	| {
			readonly type: 'error';
			readonly message: string;
	  };

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

const encodePackagePath = (packageName: string) =>
	packageName.split('/').map(encodeURIComponent).join('/');

const waitForContinueInstall = async (timeoutMs = 30_000) => {
	await new Promise<void>((resolve, reject) => {
		const timer = setTimeout(() => {
			process.off('message', onMessage);
			reject(new Error('Timed out waiting for continue-install IPC message'));
		}, timeoutMs);

		const onMessage = (message: unknown) => {
			if (!message || typeof message !== 'object') return;
			const type = Reflect.get(message, 'type');
			if (type !== 'continue-install') return;
			clearTimeout(timer);
			process.off('message', onMessage);
			resolve();
		};

		process.on('message', onMessage);
	});
};

const sendWorkerMessage = (message: ChildToParentMessage) => {
	process.send?.(message);
};

const installPackageIntoCwd = (cwd: string, packageName: string, version: string) => {
	const packageDirectory = path.join(cwd, 'node_modules', ...packageName.split('/'));
	mkdirSync(path.join(packageDirectory, 'src'), { recursive: true });
	const title = packageName === 'react' ? 'React' : packageName;
	writeFileSync(path.join(packageDirectory, 'README.md'), `# ${title}\n\nInstalled for btca`);
	writeFileSync(
		path.join(packageDirectory, 'package.json'),
		JSON.stringify({ name: packageName, version }, null, 2)
	);
	writeFileSync(path.join(packageDirectory, 'src', 'runtime.js'), `export const rune = '$state';`);
};

const installMock = (mock: WorkerMockConfig) =>
	((...spawnArgs: Parameters<typeof Bun.spawn>) => {
		const [command, options] = spawnArgs;
		const commandArgs = Array.isArray(command) ? command : [command];
		const packageSpec = commandArgs.at(-1) ?? '';
		const { packageName, version } = parsePackageSpec(packageSpec);
		const cwd = options?.cwd;

		const exited = (async () => {
			if (mock.holdInstallUntilContinue) {
				sendWorkerMessage({
					type: 'install-started',
					packageSpec,
					...(typeof cwd === 'string' ? { cwd } : {})
				});
				await waitForContinueInstall();
			}
			if ((mock.exitCode ?? 0) === 0 && cwd) {
				installPackageIntoCwd(cwd, packageName, version);
			}
			return mock.exitCode ?? 0;
		})();

		return {
			stdout: streamFromString(mock.stdout ?? ''),
			stderr: streamFromString(mock.stderr ?? ''),
			exited
		} as unknown as ReturnType<typeof Bun.spawn>;
	}) as typeof Bun.spawn;

const createFetchMock = (mock: WorkerMockConfig) =>
	(async (input: string | URL | Request) => {
		const url = String(input);
		const registryUrl = `https://registry.npmjs.org/${encodeURIComponent(mock.packageName)}`;
		const pageUrl = `https://www.npmjs.com/package/${encodePackagePath(mock.packageName)}/v/${encodeURIComponent(mock.resolvedVersion)}`;

		if (url.startsWith(registryUrl)) {
			return new Response(
				JSON.stringify({
					'dist-tags': { latest: mock.resolvedVersion },
					versions: {
						[mock.resolvedVersion]: {
							name: mock.packageName,
							version: mock.resolvedVersion,
							description: mock.packageName === 'react' ? 'React' : mock.packageName,
							readme: `# ${mock.packageName}`
						}
					}
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			);
		}

		if (url.startsWith(pageUrl)) {
			return new Response(mock.pageHtml ?? `<html><title>${mock.packageName}</title></html>`, {
				status: 200
			});
		}

		return new Response('not found', { status: 404 });
	}) as typeof fetch;

const run = async (config: WorkerConfig) => {
	const originalFetch = globalThis.fetch;
	globalThis.fetch = createFetchMock(config.mock);
	const npmDeps: Partial<NpmResourceDeps> = {
		spawn: installMock(config.mock)
	};
	setFilesystemLockTestHookForTests((event) => {
		sendWorkerMessage({
			type: 'lock-event',
			event
		});
	});

	try {
		const service = createResourcesService(
			createConfigServiceMock({
				resourcesDirectory: config.resourcesDirectory,
				resources: config.resources
			}),
			{ npm: npmDeps }
		);

		if (config.action.type === 'clear') {
			return await service.clearCachesPromise();
		}

		const resource = await service.loadPromise(config.action.reference, { quiet: true });
		if (config.action.type === 'cleanup') {
			if (!resource.cleanup) {
				throw new Error(`Resource "${config.action.reference}" does not expose cleanup()`);
			}
			await resource.cleanup();
			return { cleaned: true, name: resource.name };
		}

		const vfsId = createVirtualFs();
		try {
			const materialized = await resource.materializeIntoVirtualFs({
				destinationPath: config.action.destinationPath ?? '/resource',
				vfsId
			});
			return {
				name: resource.name,
				fsName: resource.fsName,
				metadata: materialized.metadata,
				hasCleanup: Boolean(resource.cleanup)
			};
		} finally {
			disposeVirtualFs(vfsId);
		}
	} finally {
		setFilesystemLockTestHookForTests();
		globalThis.fetch = originalFetch;
	}
};

const main = async () => {
	const configPath = process.argv[2];
	if (!configPath) {
		throw new Error('Expected worker config path argument');
	}

	const configText = await Bun.file(configPath).text();
	const config = JSON.parse(configText) as WorkerConfig;
	if (config.mock.stdout) process.stdout.write(config.mock.stdout);
	if (config.mock.stderr) process.stderr.write(config.mock.stderr);
	const result = await run(config);
	if (config.mock.exitBeforeResult) {
		process.exit(config.mock.exitCode ?? 1);
	}
	sendWorkerMessage({
		type: 'result',
		result
	});
};

if (import.meta.main) {
	try {
		await main();
	} catch (cause) {
		const message =
			cause instanceof Error ? `${cause.message}\n${cause.stack ?? ''}` : String(cause);
		sendWorkerMessage({
			type: 'error',
			message
		});
		console.error(message);
		process.exit(1);
	}
}
