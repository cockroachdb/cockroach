// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { CoordinatedUniversalTime } from "src/contexts";

import { longToInt } from "./fixLong";

export const kibi = 1024;
export const byteUnits: string[] = [
  "B",
  "KiB",
  "MiB",
  "GiB",
  "TiB",
  "PiB",
  "EiB",
  "ZiB",
  "YiB",
];

export const durationUnitsDescending = [
  { units: "hr", value: 60 * 60 * 1_000_000_000 },
  { units: "min", value: 60 * 1_000_000_000 },
  { units: "s", value: 1_000_000_000 },
  { units: "ms", value: 1_000_000 },
  { units: "µs", value: 1_000 },
  { units: "ns", value: 1 },
];

export const countUnits: string[] = ["", "k", "m", "b"];

interface UnitValue {
  value: number;
  units: string;
}

// computePrefixExponent is used to compute an appropriate display unit for a value
// for which the units have metric-style prefixes available. For example, the
// value may be expressed in bytes, but we may want to display it on the graph
// as a larger prefix unit (such as "kilobytes" or "gigabytes") in order to make
// the numbers more readable.
export function ComputePrefixExponent(
  value: number,
  prefixMultiple: number,
  prefixList: string[],
): number {
  // Compute the metric prefix that will be used to label the axis.
  let maxUnits = Math.abs(value);
  let prefixScale: number;
  for (
    prefixScale = 0;
    maxUnits >= prefixMultiple && prefixScale < prefixList.length - 1;
    prefixScale++
  ) {
    maxUnits /= prefixMultiple;
  }
  return prefixScale;
}

/**
 * ComputeByteScale calculates the appropriate scale factor and unit to use to
 * display a given byte value, without actually converting the value.
 *
 * This is used to prescale byte values before passing them to a d3-axis.
 */
export function ComputeByteScale(bytes: number): UnitValue {
  const scale = ComputePrefixExponent(bytes, kibi, byteUnits);
  return {
    value: Math.pow(kibi, scale),
    units: byteUnits[scale],
  };
}

export function BytesToUnitValue(bytes: number): UnitValue {
  const scale = ComputeByteScale(bytes);
  return {
    value: bytes / scale.value,
    units: scale.units,
  };
}

/**
 * BytesWithPrecision is like Bytes, but accepts a precision parameter
 * indicating how many digits after the decimal point are desired.
 */
export function BytesWithPrecision(bytes: number, precision: number): string {
  const unitVal = BytesToUnitValue(bytes);
  if (!unitVal.value) {
    return "0 B";
  }
  return unitVal.value.toFixed(precision) + " " + unitVal.units;
}

/**
 * Bytes creates a string representation for a number of bytes. For
 * large numbers of bytes, the value will be converted into a large unit
 * (e.g. Kibibytes, Mebibytes).
 *
 * This function was adapted from
 * https://stackoverflow.com/questions/10420352/converting-file-size-in-bytes-to-human-readable
 */
export function Bytes(bytes: number): string {
  return BytesWithPrecision(bytes, 1);
}

/**
 * Cast bytes to provided scale units
 */
// tslint:disable-next-line: variable-name
export const BytesFitScale =
  (scale: string) =>
  (bytes: number): string => {
    if (!bytes) {
      return `0.00 ${scale}`;
    }
    const n = byteUnits.indexOf(scale);
    return `${(bytes / Math.pow(kibi, n)).toFixed(2)} ${scale}`;
  };

/**
 * Percentage creates a string representation of a fraction as a percentage.
 * Accepts a precision parameter as optional indicating how many digits
 * after the decimal point are desired. (e.g. precision 2 returns 8.37 %)
 */
export function Percentage(
  numerator: number,
  denominator: number,
  precision?: number,
): string {
  if (denominator === 0) {
    return "--%";
  }
  if (precision) {
    return ((numerator / denominator) * 100).toFixed(precision) + " %";
  }
  return Math.floor((numerator / denominator) * 100).toString() + " %";
}

/**
 * PercentageCustom creates a string representation of a fraction as a percentage.
 * Accepts a precision parameter as optional indicating how many digits
 * after the decimal point are desired. (e.g. precision 2 returns 8.37 %)
 * If the number is zero or grater than 1, it works the same way as the Percentage
 * function above, if is between 0 and 1, it looks for the first non-zero
 * decimal number, up to 10 decimal points, otherwise shows ~0.0%
 */
export function PercentageCustom(
  numerator: number,
  denominator: number,
  precision?: number,
): string {
  if (denominator === 0) {
    return "--%";
  }
  const pct = (numerator / denominator) * 100;
  if (pct <= 0 || pct >= 1) {
    return Percentage(numerator, denominator, precision);
  }
  const pctString = pct.toFixed(10);
  let finalPct = "0.";
  let found = false;
  for (let index = 2; index < pctString.length && !found; index++) {
    finalPct = `${finalPct}${pctString[index]}`;
    if (pctString[index] !== "0") {
      found = true;
    }
  }
  if (found) {
    return finalPct + " %";
  }
  return "~0.0 %";
}

/**
 * ComputeDurationScale calculates an appropriate scale factor and unit to use
 * to display a given duration value, without actually converting the value.
 */
export function ComputeDurationScale(ns: number): UnitValue {
  return durationUnitsDescending.find(
    ({ value }) => ns / value >= 1 || value === 1,
  );
}

/**
 * Duration creates a string representation for a duration. The expectation is
 * that units are passed in nanoseconds; for larger durations, the value will
 * be converted into larger units.
 */
export function Duration(nanoseconds: number): string {
  const scale = ComputeDurationScale(nanoseconds);
  const unitVal = nanoseconds / scale.value;
  return unitVal.toFixed(1) + " " + scale.units;
}

/**
 * Duration creates a string representation for a duration. The expectation is
 * that units are passed in nanoseconds; for larger durations, the value will
 * be converted into larger units.
 * If the value is 0, return "no samples".
 */
export function DurationCheckSample(nanoseconds: number): string {
  if (nanoseconds === 0) {
    return "no samples";
  }
  return Duration(nanoseconds);
}

export const DATE_FORMAT = "MMM DD, YYYY [at] H:mm";
export const DATE_WITH_SECONDS_FORMAT = "MMM DD, YYYY [at] H:mm:ss";
export const DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT =
  "MMM DD, YYYY [at] H:mm:ss.ms";

/**
 * Alternate 24 hour formats
 */
export const DATE_FORMAT_24_TZ = "MMM DD, YYYY [at] H:mm z";
export const DATE_WITH_SECONDS_FORMAT_24_TZ = "MMM DD, YYYY [at] H:mm:ss z";
export const DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ =
  "MMM DD, YYYY [at] H:mm:ss.ms z";

export function FormatWithTimezone(
  m: moment.Moment,
  formatString = DATE_WITH_SECONDS_FORMAT_24_TZ,
  timezone = CoordinatedUniversalTime,
): string {
  return moment.tz(m, timezone).format(formatString);
}

export function RenderCount(yesCount: Long, totalCount: Long): string {
  if (longToInt(yesCount) === 0) {
    return "No";
  }
  if (longToInt(yesCount) === longToInt(totalCount)) {
    return "Yes";
  }
  const noCount = longToInt(totalCount) - longToInt(yesCount);
  return `${longToInt(yesCount)} Yes / ${noCount} No`;
}

/**
 * ComputeCountScale calculates an appropriate scale factor and unit to use
 * to display a given count value, without actually converting the value.
 */
function ComputeCountScale(count: number): UnitValue {
  const scale = ComputePrefixExponent(count, 1000, countUnits);
  return {
    value: Math.pow(1000, scale),
    units: countUnits[scale],
  };
}

/**
 * Count creates a string representation for a count.
 */
export function Count(count: number): string {
  const scale = ComputeCountScale(count);
  const unitVal = count / scale.value;
  const fractionDigits = Number.isInteger(unitVal) ? 0 : 1;
  return unitVal.toFixed(fractionDigits) + " " + scale.units;
}

// limitText returns a shortened form of text that surpasses a given limit
export const limitText = (text: string, limit: number): string => {
  if (!text) {
    return "";
  }
  return text?.length > limit ? text.slice(0, limit - 3).concat("...") : text;
};

// limitStringArray returns a shortened form of text that surpasses a given limit
export const limitStringArray = (arr: string[], limit: number): string => {
  if (!arr || arr.length === 0) {
    return "";
  }

  // Remove null and undefined entries in the array.
  arr = arr.filter(n => n);
  if (arr.length === 0) {
    return "";
  }

  if (arr.length === 1 || arr[0]?.length > limit) {
    return limitText(arr[0], limit);
  }

  let str = arr[0];
  for (let next = 1; next < arr.length; ++next) {
    const charsLeft = limit - str.length;
    if (charsLeft < arr[next].length) {
      str += arr[next].substring(0, charsLeft).concat("...");
      break;
    }
    str += arr[next];
  }

  return str;
};

function add(a: string, b: string): string {
  let c = 0;
  const r = [];
  const x = a.split("").map(Number);
  const y = b.split("").map(Number);
  while (x.length || y.length) {
    const s = (x.pop() || 0) + (y.pop() || 0) + c;
    r.unshift(s < 10 ? s : s - 10);
    c = s < 10 ? 0 : 1;
  }
  if (c) r.unshift(c);
  return r.join("");
}

// HexStringToInt64String converts a hex value (in string form)
// to an int64 (in string form).
export function HexStringToInt64String(s: string): string {
  let dec = "0";
  s?.split("").forEach(function (chr: string) {
    const n = parseInt(chr, 16);
    for (let t = 8; t; t >>= 1) {
      dec = add(dec, dec);
      if (n & t) dec = add(dec, "1");
    }
  });
  return dec;
}

// FixFingerprintHexValue adds the leading 0 on strings with hex value that
// have a length < 16. This can occur because it was returned like this from the DB
// or because the hex value was generated using `.toString(16)` (which removes the
// leading zeros).
// The zeros need to be added back to match the value on our sql stats tables.
export function FixFingerprintHexValue(s: string): string {
  if (s === undefined || s === null || s.length === 0) {
    return "";
  }
  while (s.length < 16) {
    s = `0${s}`;
  }
  return s;
}

// capitalize capitalizes a string.
export function capitalize(str: string): string {
  if (!str) return str;
  return str[0].toUpperCase() + str.substring(1);
}

export function EncodeUriName(name: string): string {
  // When a string has a '%' on it, the URI needs to have '%2525' instead, so this
  // function replaces '%25' with '%252525' because when the link is created
  // we have [%25]2525 -> %2525 (which is then used as the URI)
  return encodeURIComponent(name).replace(/%25/g, "%252525");
}

function encodeDatabasesUri(db: string): string {
  return `/databases/${EncodeUriName(db)}`;
}

export function EncodeDatabasesToIndexUri(
  db: string,
  schema: string,
  table: string,
  indexName: string,
): string {
  return `${encodeDatabasesUri(db)}/${EncodeUriName(schema)}/${EncodeUriName(
    table,
  )}/${EncodeUriName(indexName)}`;
}

function encodeDatabaseTableUri(db: string, table: string): string {
  return `${encodeDatabaseUri(db)}/table/${EncodeUriName(table)}`;
}

export function EncodeDatabaseTableIndexUri(
  db: string,
  table: string,
  indexName: string,
): string {
  return `${encodeDatabaseTableUri(db, table)}/index/${EncodeUriName(
    indexName,
  )}`;
}

function encodeDatabaseUri(db: string): string {
  return `/database/${EncodeUriName(db)}`;
}

interface BreakLineReplacement {
  [key: string]: string;
}

const breakLinesKeywords: BreakLineReplacement = {
  " FROM ": " FROM ",
  " WHERE ": "   WHERE ",
  " AND ": "    AND ",
  " ORDER ": " ORDER ",
  " LIMIT ": " LIMIT ",
  " JOIN ": "   JOIN ",
  " ON ": "    ON ",
  " VALUES ": "   VALUES ",
};
const LINE_BREAK_LIMIT = 100;

export function FormatQuery(query: string): string {
  if (query == null) {
    return "";
  }
  Object.keys(breakLinesKeywords).forEach(key => {
    query = query.replace(new RegExp(key, "g"), `\n${breakLinesKeywords[key]}`);
  });
  const lines = query.split("\n").map(line => {
    if (line.length <= LINE_BREAK_LIMIT) {
      return line;
    }
    return breakLongLine(line, LINE_BREAK_LIMIT);
  });

  return lines.join("\n");
}

function breakLongLine(line: string, limit: number): string {
  if (line.length <= limit) {
    return line;
  }
  const idxComma = line.indexOf(",", limit);
  if (idxComma === -1) {
    return line;
  }

  return `${line.substring(0, idxComma + 1)}\n${breakLongLine(
    line.substring(idxComma + 1).trim(),
    limit,
  )}`;
}
