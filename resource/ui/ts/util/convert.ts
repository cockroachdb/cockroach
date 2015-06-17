// source: util/convert.ts
// Author: Matt Tracy (matt@cockroachlabs.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * Utils contains common utilities.
 */
module Utils {
	"use strict";

	export module Convert {
		/**
		 * MilliToNano is intended to convert milliseconds (used by Java time
		 * methods) to nanoseconds, which are used by the cockroach server.
		 */
		export function MilliToNano(millis: number): number {
			return millis * 1.0e6;
		}

		/**
		 * NanosToMilli converts a nanoseconds to milliseconds, for use in Java
		 * time methods.
		 */
		export function NanoToMilli(nano: number): number {
			return nano / 1.0e6;
		}
	}
}
