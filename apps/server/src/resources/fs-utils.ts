import { rm, stat } from 'node:fs/promises';

export const pathExists = async (targetPath: string) => {
	try {
		await stat(targetPath);
		return true;
	} catch {
		return false;
	}
};

export const directoryExists = async (targetPath: string) => {
	try {
		const stats = await stat(targetPath);
		return stats.isDirectory();
	} catch {
		return false;
	}
};

export const cleanupDirectory = async (pathToRemove: string) => {
	try {
		await rm(pathToRemove, { recursive: true, force: true });
	} catch {
		return;
	}
};
