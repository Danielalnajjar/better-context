import { describe, expect, it } from 'bun:test';

import { CollectionError } from './collections/types.ts';
import { formatErrorForDisplay, getErrorHint, getErrorMessage, getErrorTag } from './errors.ts';
import { ResourceError } from './resources/helpers.ts';

describe('errors', () => {
	it('unwraps panic wrappers to the underlying tagged error details', () => {
		const providerError = Object.assign(new Error('Provider "opencode" is not authenticated.'), {
			_tag: 'ProviderNotAuthenticatedError',
			hint: 'Run "opencode auth" to configure provider credentials.'
		});
		const wrapped = {
			_tag: 'Panic',
			message: 'match err handler threw',
			cause: {
				_tag: 'AgentError',
				message: 'Failed to get response from AI',
				hint: 'This may be a temporary issue. Try running the command again.',
				cause: {
					_tag: 'UnhandledException',
					message: 'Unhandled exception: Provider "opencode" is not authenticated.',
					cause: providerError
				}
			}
		};

		expect(getErrorTag(wrapped)).toBe('ProviderNotAuthenticatedError');
		expect(getErrorMessage(wrapped)).toBe('Provider "opencode" is not authenticated.');
		expect(getErrorHint(wrapped)).toBe('Run "opencode auth" to configure provider credentials.');
		expect(formatErrorForDisplay(wrapped)).toBe(
			'Provider "opencode" is not authenticated.\n\nHint: Run "opencode auth" to configure provider credentials.'
		);
	});

	it('keeps contextual non-wrapper error messages', () => {
		const error = {
			_tag: 'CollectionError',
			message: 'Failed to load resource "npm:prettier": missing docs index',
			hint: 'Try running "btca clear" and reload resources.',
			cause: {
				_tag: 'Panic',
				message: 'match err handler threw',
				cause: new Error('boom')
			}
		};

		expect(getErrorTag(error)).toBe('CollectionError');
		expect(getErrorMessage(error)).toBe(
			'Failed to load resource "npm:prettier": missing docs index'
		);
		expect(getErrorHint(error)).toBe('Try running "btca clear" and reload resources.');
	});

	it('formats structured collection errors with contextual wrapper text plus cause details', () => {
		const error = {
			_tag: 'CollectionError',
			code: 'RESOURCE_MATERIALIZE_FAILED',
			resourceName: 'broken-docs',
			message: 'Failed to materialize resource "broken-docs"',
			hint: 'Verify the branch name exists in the repository.',
			cause: {
				_tag: 'ResourceError',
				message: 'Branch "missing" not found in the repository',
				hint: 'Verify the branch name exists in the repository.'
			}
		};

		expect(getErrorTag(error)).toBe('CollectionError');
		expect(getErrorMessage(error)).toBe(
			'Failed to materialize resource "broken-docs": Branch "missing" not found in the repository'
		);
		expect(getErrorHint(error)).toBe('Verify the branch name exists in the repository.');
	});

	it('preserves cause on real CollectionError instances without reassigning it', () => {
		const cause = new ResourceError({
			message: 'Branch "missing" not found in the repository',
			hint: 'Verify the branch name exists in the repository.'
		});
		const error = new CollectionError({
			message: 'Failed to materialize resource "broken-docs"',
			code: 'RESOURCE_MATERIALIZE_FAILED',
			resourceName: 'broken-docs',
			cause
		});

		expect(error.cause).toBe(cause);
		expect(getErrorTag(error)).toBe('CollectionError');
		expect(getErrorMessage(error)).toBe(
			'Failed to materialize resource "broken-docs": Branch "missing" not found in the repository'
		);
		expect(getErrorHint(error)).toBe('Verify the branch name exists in the repository.');
		expect(formatErrorForDisplay(error)).toBe(
			'Failed to materialize resource "broken-docs": Branch "missing" not found in the repository\n\nHint: Verify the branch name exists in the repository.'
		);
	});

	it('handles cyclic cause chains without throwing', () => {
		const cyclic = { _tag: 'Panic', message: 'match err handler threw' } as {
			_tag: string;
			message: string;
			cause?: unknown;
		};
		cyclic.cause = cyclic;

		expect(getErrorTag(cyclic)).toBe('Panic');
		expect(getErrorMessage(cyclic)).toBe('match err handler threw');
		expect(getErrorHint(cyclic)).toBeUndefined();
	});
});
