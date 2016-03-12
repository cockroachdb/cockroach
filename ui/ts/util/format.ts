// source: util/format.ts
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../models/proto.ts" />
/// <reference path="../util/convert.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * Utils contains common utilities.
 */
module Utils {
  "use strict";
  /**
   * Formatter contains common code for converting numbers and dates to human
   * readable formats.
   */
  export module Format {
    /**
     * Date formats a Date object into a human readable date string.
     */
    const _datetimeFormatter: d3.time.Format = d3.time.format("%Y-%m-%d %H:%M:%S");
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
     * Bytes creates a string representation for a number of bytes. For
     * large numbers of bytes, the value will be converted into a large unit
     * (e.g. Kibibytes, Mebibytes).
     *
     * This function was adapted from
     * https://stackoverflow.com/questions/10420352/converting-file-size-in-bytes-to-human-readable
     */
    const kibi: number = 1024;
    const units: string[] = ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];
    export interface UnitValue {
      value: number;
      units: string;
    }

    export function BytesToUnitValue(bytes: number): UnitValue {
      if (Math.abs(bytes) < kibi) {
        return {value: bytes, units: "B"};
      }
      let u: number = -1;
      do {
        bytes /= kibi;
        ++u;
      } while (Math.abs(bytes) >= kibi && u < units.length - 1);
      return {
        value: bytes,
        units: units[u],
      };
    }

    export function Bytes(bytes: number): string {
      let unitVal: UnitValue = BytesToUnitValue(bytes);
      return unitVal.value.toFixed(1) + " " + unitVal.units;
    }

    /**
     * Percentage creates a string representation of a fraction as a percentage.
     */
    export function Percentage(numerator: number, denominator: number): string {
      if (denominator === 0) {
        return "100%";
      }
      return Math.floor(numerator / denominator * 100).toString() + "%";
    }

    export function Titlecase(s: string): string {
      return s.replace(/_/g, " ").replace(/\b\w/g, (c: string): string => c.toUpperCase());
    }
  }
}
