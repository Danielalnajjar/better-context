import { Result } from 'better-result';
import { Command } from 'commander';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import * as readline from 'readline';
import { bold, dim, red, yellow } from '../lib/utils/colors.ts';

const home = path.resolve(os.homedir());
const configFilenames = new Set(['btca.config.jsonc', 'btca.remote.config.jsonc']);
const skipDirs = new Set([
	'.git',
	'.hg',
	'.svn',
	'node_modules',
	'.next',
	'.turbo',
	'.cache',
	'.npm',
	'.pnpm-store',
	'.bun',
	'Library',
	'Applications',
	'System',
	'Volumes',
	'private'
]);

const expandHome = (filePath: string) =>
	filePath.startsWith('~/') ? path.join(home, filePath.slice(2)) : filePath;

const opencodeAuthPath =
	process.platform === 'win32'
		? path.join(
				process.env.APPDATA || path.join(home, 'AppData', 'Roaming'),
				'opencode',
				'auth.json'
			)
		: path.join(
				process.env.XDG_DATA_HOME || path.join(home, '.local', 'share'),
				'opencode',
				'auth.json'
			);

const globalConfigDir = expandHome('~/.config/btca');
const globalDataDir = expandHome('~/.local/share/btca');

const createRl = () =>
	readline.createInterface({
		input: process.stdin,
		output: process.stdout
	});

const promptInput = (rl: readline.Interface, question: string) =>
	new Promise<string>((resolve) => {
		rl.question(question, (answer) => resolve(answer.trim()));
	});

const stripJsonc = (content: string) => {
	let out = '';
	let i = 0;
	let inString = false;
	let quote: '"' | "'" | null = null;
	let escaped = false;

	while (i < content.length) {
		const ch = content[i] ?? '';
		const next = content[i + 1] ?? '';

		if (inString) {
			out += ch;
			if (escaped) escaped = false;
			else if (ch === '\\') escaped = true;
			else if (quote && ch === quote) {
				inString = false;
				quote = null;
			}
			i += 1;
			continue;
		}

		if (ch === '/' && next === '/') {
			i += 2;
			while (i < content.length && content[i] !== '\n') i += 1;
			continue;
		}

		if (ch === '/' && next === '*') {
			i += 2;
			while (i < content.length) {
				if (content[i] === '*' && content[i + 1] === '/') {
					i += 2;
					break;
				}
				i += 1;
			}
			continue;
		}

		if (ch === '"' || ch === "'") {
			inString = true;
			quote = ch;
		}

		out += ch;
		i += 1;
	}

	let normalized = '';
	i = 0;
	inString = false;
	quote = null;
	escaped = false;

	while (i < out.length) {
		const ch = out[i] ?? '';

		if (inString) {
			normalized += ch;
			if (escaped) escaped = false;
			else if (ch === '\\') escaped = true;
			else if (quote && ch === quote) {
				inString = false;
				quote = null;
			}
			i += 1;
			continue;
		}

		if (ch === '"' || ch === "'") {
			inString = true;
			quote = ch;
			normalized += ch;
			i += 1;
			continue;
		}

		if (ch === ',') {
			let j = i + 1;
			while (j < out.length && /\s/.test(out[j] ?? '')) j += 1;
			const nextNonWs = out[j] ?? '';
			if (nextNonWs === ']' || nextNonWs === '}') {
				i += 1;
				continue;
			}
		}

		normalized += ch;
		i += 1;
	}

	return normalized.trim();
};

const parseConfig = async (configPath: string) => {
	try {
		const text = await Bun.file(configPath).text();
		return JSON.parse(stripJsonc(text)) as { dataDirectory?: unknown };
	} catch {
		return null;
	}
};

const resolveDataDirectory = (rawPath: string, configPath: string) => {
	const expanded = expandHome(rawPath);
	return path.isAbsolute(expanded)
		? path.resolve(expanded)
		: path.resolve(path.dirname(configPath), expanded);
};

const isWithin = (parentPath: string, childPath: string) => {
	const rel = path.relative(parentPath, childPath);
	return rel === '' || (!rel.startsWith('..') && !path.isAbsolute(rel));
};

const getSafetyError = (target: string, configPath?: string) => {
	const resolved = path.resolve(target);
	if (resolved === path.parse(resolved).root) return 'target is filesystem root';
	if (resolved === home) return 'target is home directory';
	if (!configPath) return null;

	const projectDir = path.resolve(path.dirname(configPath));
	if (resolved === projectDir) return 'target equals project root from config';
	if (isWithin(resolved, projectDir)) return 'target is an ancestor of project root';
	return null;
};

const readDirSafe = async (directory: string) => {
	try {
		return await fs.readdir(directory, { withFileTypes: true });
	} catch {
		return null;
	}
};

const discoverLocalState = async () => {
	const configFiles: string[] = [];
	const legacyDataDirs: string[] = [];
	const stack = [home];

	while (stack.length > 0) {
		const current = stack.pop()!;
		const entries = await readDirSafe(current);
		if (!entries) continue;

		for (const entry of entries) {
			if (entry.isSymbolicLink()) continue;
			const fullPath = path.join(current, entry.name);

			if (entry.isDirectory()) {
				if (entry.name === '.btca') {
					legacyDataDirs.push(fullPath);
					continue;
				}
				if (skipDirs.has(entry.name)) continue;
				stack.push(fullPath);
				continue;
			}

			if (entry.isFile() && configFilenames.has(entry.name)) {
				configFiles.push(fullPath);
			}
		}
	}

	return { configFiles, legacyDataDirs };
};

const runWipe = async () => {
	const targets = new Map<string, { source: string; configPath?: string }>();
	const addTarget = (targetPath: string, source: string, configPath?: string) => {
		targets.set(path.resolve(targetPath), { source, configPath });
	};

	addTarget(globalConfigDir, 'global BTCA config directory');
	addTarget(globalDataDir, 'global BTCA data directory');
	addTarget(opencodeAuthPath, 'provider auth file (opencode)');

	const { configFiles, legacyDataDirs } = await discoverLocalState();

	for (const configPath of configFiles) {
		addTarget(configPath, 'project BTCA config file');
		if (path.basename(configPath) !== 'btca.config.jsonc') continue;

		const config = await parseConfig(configPath);
		const rawDataDirectory = config?.dataDirectory;
		if (typeof rawDataDirectory !== 'string' || rawDataDirectory.trim() === '') continue;

		addTarget(
			resolveDataDirectory(rawDataDirectory.trim(), configPath),
			`dataDirectory from ${configPath}`,
			configPath
		);
	}

	for (const dataDir of legacyDataDirs) {
		addTarget(dataDir, 'legacy local data directory (.btca)');
	}

	const removed: string[] = [];
	const skipped: string[] = [];
	const failed: string[] = [];
	const orderedTargets = [...targets.entries()].sort(([a], [b]) => b.length - a.length);

	for (const [target, meta] of orderedTargets) {
		const safetyError = getSafetyError(target, meta.configPath);
		if (safetyError) {
			skipped.push(`${target} (${safetyError})`);
			continue;
		}

		try {
			await fs.rm(target, { recursive: true, force: true, maxRetries: 2 });
			removed.push(`${target} (${meta.source})`);
		} catch (error) {
			failed.push(
				`${target} (${meta.source}) -> ${error instanceof Error ? error.message : String(error)}`
			);
		}
	}

	return { removed, skipped, failed };
};

const confirmWipe = async () => {
	if (!process.stdin.isTTY || !process.stdout.isTTY) {
		console.error('Refusing to run wipe in non-interactive mode without --yes.');
		process.exit(1);
	}

	console.log(red('\nWARNING: this will permanently delete local BTCA data and auth.'));
	console.log(yellow('This signs out all local providers and removes BTCA project/global state.'));
	console.log(dim(`Scan root: ${home}`));
	console.log('');

	const rl = createRl();
	try {
		const answer = await promptInput(rl, `Type ${bold('WIPE')} to continue: `);
		if (answer !== 'WIPE') {
			console.log('Cancelled.');
			process.exit(0);
		}
	} finally {
		rl.close();
	}
};

const printReport = (result: Awaited<ReturnType<typeof runWipe>>) => {
	console.log('\nBTCA wipe complete.');
	console.log(`Scanned: ${home}`);
	console.log(`Removed: ${result.removed.length}`);
	console.log(`Skipped: ${result.skipped.length}`);
	console.log(`Failed: ${result.failed.length}`);

	if (result.removed.length) {
		console.log('\nRemoved paths:');
		for (const line of result.removed) console.log(`- ${line}`);
	}

	if (result.skipped.length) {
		console.log('\nSkipped paths:');
		for (const line of result.skipped) console.log(`- ${line}`);
	}

	if (result.failed.length) {
		console.log('\nFailed removals:');
		for (const line of result.failed) console.log(`- ${line}`);
	}
};

export const wipeCommand = new Command('wipe')
	.description('Delete all local BTCA config, data, cloned resources, and provider auth')
	.option('-y, --yes', 'Skip confirmation prompt')
	.action(async (options: { yes?: boolean }) => {
		const result = await Result.tryPromise(async () => {
			if (!options.yes) {
				await confirmWipe();
			}

			const wipeResult = await runWipe();
			printReport(wipeResult);
			if (wipeResult.failed.length > 0) {
				process.exitCode = 1;
			}
		});

		if (Result.isError(result)) {
			console.error(
				`Error: ${result.error instanceof Error ? result.error.message : String(result.error)}`
			);
			process.exit(1);
		}
	});
