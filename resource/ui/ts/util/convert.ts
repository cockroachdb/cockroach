// source: util/convert.ts
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * Utils contains common utilities.
 */
module Utils {
    /**
     * microToNanos is intended to convert milliseconds (used by Java time
     * methods) to nanoseconds, which are used by the cockroach server.
     */
    export function milliToNanos(millis:number):number {
        return millis*1.0e6;
    }
}
