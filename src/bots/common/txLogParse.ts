export function isIxLog(log: string): boolean {
	const match = log.match(new RegExp('Program log: Instruction:'));

	return match !== null;
}

export function isEndIxLog(programId: string, log: string): boolean {
	const match = log.match(
		new RegExp(
			`Program ${programId} consumed ([0-9]+) of ([0-9]+) compute units`
		)
	);

	return match !== null;
}

export function isFillIxLog(log: string): boolean {
	const match = log.match(
		new RegExp('Program log: Instruction: Fill(.*)Order')
	);

	return match !== null;
}

export function isOrderDoesNotExistLog(log: string): number | null {
	const match = log.match(new RegExp('.*Order does not exist ([0-9]+)'));

	if (!match) {
		return null;
	}

	return parseInt(match[1]);
}

export function isMakerOrderDoesNotExistLog(log: string): number | null {
	const match = log.match(new RegExp('.*Maker has no order id ([0-9]+)'));

	if (!match) {
		return null;
	}

	return parseInt(match[1]);
}

/**
 * parses a maker breached maintenance margin log, returns the maker's userAccount pubkey if it exists
 * @param log
 * @returns
 */
export function isMakerBreachedMaintenanceMarginLog(
	log: string
): string | null {
	const regex =
		/^maker \(([1-9A-HJ-NP-Za-km-z]+)\) breached maintenance requirements.*$/;
	const match = log.match(regex);

	return match ? match[1] : null;
}

export function isTakerBreachedMaintenanceMarginLog(log: string): boolean {
	const match = log.match(
		new RegExp('.*taker breached maintenance requirements.*')
	);

	return match !== null;
}

export function isErrFillingLog(log: string): [string, string] | null {
	const match = log.match(
		new RegExp('.*Err filling order id ([0-9]+) for user ([a-zA-Z0-9]+)')
	);

	if (!match) {
		return null;
	}

	return [match[1], match[2]];
}

export function isErrStaleOracle(log: string): boolean {
	const match = log.match(new RegExp('.*Invalid Oracle: Stale.*'));

	if (!match) {
		return false;
	}

	return true;
}
