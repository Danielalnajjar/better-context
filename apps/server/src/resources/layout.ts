import path from 'node:path';

export const RESOURCE_LOCKS_DIR = '.resource-locks';
export const CLEAR_LOCK_NAME = 'clear.lock';
export const GIT_MIRRORS_DIR = '.git-mirrors';
export const TMP_DIR = '.tmp';
export const CLEAR_TRASH_DIR = '.clear-trash';

export const getResourceLocksRoot = (resourcesDirectory: string) =>
	path.join(resourcesDirectory, RESOURCE_LOCKS_DIR);

export const getClearLockPath = (resourcesDirectory: string) =>
	path.join(getResourceLocksRoot(resourcesDirectory), CLEAR_LOCK_NAME);

export const getGitLockPath = (resourcesDirectory: string, key: string) =>
	path.join(getResourceLocksRoot(resourcesDirectory), `git.${key}.lock`);

export const getNpmLockPath = (resourcesDirectory: string, key: string) =>
	path.join(getResourceLocksRoot(resourcesDirectory), `npm.${key}.lock`);

export const getGitMirrorRoot = (resourcesDirectory: string) =>
	path.join(resourcesDirectory, GIT_MIRRORS_DIR);

export const getGitMirrorPath = (resourcesDirectory: string, key: string) =>
	path.join(getGitMirrorRoot(resourcesDirectory), key);

export const getGitMirrorRepoPath = (resourcesDirectory: string, key: string) =>
	path.join(getGitMirrorPath(resourcesDirectory, key), 'repo');

export const getTopLevelCachePath = (resourcesDirectory: string, key: string) =>
	path.join(resourcesDirectory, key);

export const getClearTrashRoot = (resourcesDirectory: string) =>
	path.join(resourcesDirectory, CLEAR_TRASH_DIR);

export const getTmpCacheRoot = (resourcesDirectory: string) =>
	path.join(resourcesDirectory, TMP_DIR);

export const getTmpCachePath = (resourcesDirectory: string, key: string) =>
	path.join(getTmpCacheRoot(resourcesDirectory), key);
