// source: util/format.ts
/// <reference path="../util/convert.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * Utils contains common utilities.
 */
module Utils {
    /**
     * Formatter contains common code for converting numbers and dates to human
     * readable formats.
     */
    export module Format {
        /**
         * DateFromDate formats a Date object into a human readable date
         * string.
         */
        var _datetimeFormatter = d3.time.format("%Y-%m-%d %H:%M:%S");
        export function DateFromDate(datetime: Date): string {
            return _datetimeFormatter(datetime);
        }

        /**
         * DateFromTimestamp formats a timestamp (nano) into a human readable
         * date string.
         */
        export function DateFromTimestamp(timestamp: number): string {
            var datetime = Convert.TimestampToDate(timestamp);
            return DateFromDate(datetime);
        }

        /**
         * SeverityFromNumber formats a numerical severity into its string
         * representation.
         */
        var _severities = {
            0 : "INFO",
            1 : "WARNING",
            2 : "ERROR",
            3 : "FATAL"
        };
        export function SeverityFromNumber(severity: number): string {
            return _severities[severity];
        }
    }
}
