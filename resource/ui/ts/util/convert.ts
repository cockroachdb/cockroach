// source: util/convert.ts
// Author: Matt Tracy (matt@cockroachlabs.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * Utils contains common utilities.
 */
module Utils {
	export module Convert {
		/**
		 * microToNanos is intended to convert milliseconds (used by Java time
		 * methods) to nanoseconds, which are used by the cockroach server.
		 */
		export function MilliToNanos(millis: number): number {
			return millis * 1.0e6;
		}

		/**
		 * TimestampToDate converts a timestamp (nanos) into a Date.
		 */
		export function TimestampToDate(timestamp: number): Date {
			return new Date(timestamp / 1.0e6);
		}
	}
}
