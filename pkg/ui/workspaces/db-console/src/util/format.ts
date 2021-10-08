// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
export const durationUnits: string[] = ["ns", "µs", "ms", "s"];

interface UnitValue {
  value: number;
  units: string;
}

// computePrefixExponent is used to compute an appopriate display unit for a value
// for which the units have metric-style prefixes available. For example, the
// value may be expressed in bytes, but we may want to display it on the graph
// as a larger prefix unit (such as "kilobytes" or "gigabytes") in order to make
// the numbers more readable.
export function ComputePrefixExponent(
  value: number,
  prefixMultiple: number,
  prefixList: string[],
) {
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
 * Cast bytes to provided scale units
 */
export const BytesFitScale = (scale: string) => (bytes: number) => {
  if (!bytes) {
    return `0.00 ${scale}`;
  }
  const n = byteUnits.indexOf(scale);
  return `${(bytes / Math.pow(kibi, n)).toFixed(2)} ${scale}`;
};

/**
 * Percentage creates a string representation of a fraction as a percentage.
 */
export function Percentage(numerator: number, denominator: number): string {
  if (denominator === 0) {
    return "--%";
  }
  return Math.floor((numerator / denominator) * 100).toString() + "%";
}

/**
 * ComputeDurationScale calculates an appropriate scale factor and unit to use
 * to display a given duration value, without actually converting the value.
 */
export function ComputeDurationScale(nanoseconds: number): UnitValue {
  const scale = ComputePrefixExponent(nanoseconds, 1000, durationUnits);
  return {
    value: Math.pow(1000, scale),
    units: durationUnits[scale],
  };
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
 * Cast nanonseconds to provided scale units
 */
export const DurationFitScale = (scale: string) => (nanoseconds: number) => {
  if (!nanoseconds) {
    return `0.00 ${scale}`;
  }
  const n = durationUnits.indexOf(scale);
  return `${(nanoseconds / Math.pow(1000, n)).toFixed(2)} ${scale}`;
};

export const DATE_FORMAT = "MMM DD, YYYY [at] h:mm A";

/**
 * Alternate 24 hour UTC format
 */
export const DATE_FORMAT_24_UTC = "MMM DD, YYYY [at] HH:mm UTC";
