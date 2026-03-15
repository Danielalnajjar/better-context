export const readStream = async (stream: ReadableStream<Uint8Array> | null) => {
	if (!stream) return '';
	return await new Response(stream).text();
};

export const waitFor = async (predicate: () => boolean | Promise<boolean>, timeoutMs = 4_000) => {
	const startedAt = performance.now();
	while (performance.now() - startedAt < timeoutMs) {
		if (await predicate()) return;
		await Bun.sleep(10);
	}
	throw new Error('Timed out waiting for condition');
};

export const logQueueMetrics = (
	label: string,
	metrics: Record<string, number | readonly number[]>
) => {
	const roundMetricValue = (value: number | readonly number[]) => {
		if (typeof value === 'number') {
			return Number(value.toFixed(2));
		}
		return value.map((entry) => Number(entry.toFixed(2)));
	};

	console.info(
		`[btca queue-metrics] ${JSON.stringify({
			label,
			...Object.fromEntries(
				Object.entries(metrics).map(([key, value]) => [key, roundMetricValue(value)])
			)
		})}`
	);
};
