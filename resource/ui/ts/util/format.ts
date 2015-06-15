// source: util/format.ts
/// <reference path="../models/proto.ts" />
/// <reference path="../util/convert.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

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
         * Date formats a Date object into a human readable date string.
         */
        var _datetimeFormatter = d3.time.format("%Y-%m-%d %H:%M:%S");
        export function Date(datetime: Date): string {
            return _datetimeFormatter(datetime);
        };

        /**
         * Severity formats a numerical severity into its string
         * representation.
         */
        enum Severities {
            INFO = 0,
            WARNING = 1,
            ERROR = 2,
            FATAL = 3
        };
        export function Severity(severity: number): string {
            return Severities[severity];
        };

        /**
         * LogEntryMessage formats a single log entry into a human readable format.
         */
        var _messageTags = new RegExp("%s|%d|%v|%+v", "gi")
        export function LogEntryMessage(entry: Models.Proto.LogEntry): string {
            var i = -1;
            return entry.format.replace(_messageTags, function(tag) {
                i++;
                if (entry.args.length > i) {
                    return entry.args[i].str;
                } else {
                    return "";
                }
            });
        };

        /** 
         * Bytes creates a string representation for a number of bytes. For
         * large numbers of bytes, the value will be converted into a large unit
         * (e.g. Kibibytes, Mebibytes).
         * 
         * This function was adapted from 
         * https://stackoverflow.com/questions/10420352/converting-file-size-in-bytes-to-human-readable
         */
        var kibi = 1024
        var units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
        export function Bytes(bytes: number): string {
            if (Math.abs(bytes) < kibi) {
                return bytes + ' B';
            }
            var u = -1;
            do {
                bytes /= kibi;
                ++u;
            } while (Math.abs(bytes) >= kibi && u < units.length - 1);
            return bytes.toFixed(1) + ' ' + units[u];
        }
    }
}
