import d3 from "d3";

export const kibi = 1024;
const byteUnits: string[] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];
const durationUnits: string[] = ["ns", "µs", "ms", "s"];

interface UnitValue {
  value: number;
  units: string;
}

// computePrefixExponent is used to compute an appopriate display unit for a value
// for which the units have metric-style prefixes available. For example, the
// value may be expressed in bytes, but we may want to display it on the graph
// as a larger prefix unit (such as "kilobytes" or "gigabytes") in order to make
// the numbers more readable.
export function ComputePrefixExponent(value: number, prefixMultiple: number, prefixList: string[]) {
  // Compute the metric prefix that will be used to label the axis.
  let maxUnits = Math.abs(value);
  let prefixScale: number;
  for (prefixScale = 0;
       maxUnits >= prefixMultiple && prefixScale < (prefixList.length - 1);
       prefixScale++) {
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
  const unitVal = BytesToUnitValue(bytes);
  if (!unitVal.value) {
    return "0 B";
  }
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

/**
 * Duration creates a string representation for a duration. The expectation is
 * that units are passed in nanoseconds; for larger durations, the value will
 * be converted into larger units.
 */
export function Duration(nanoseconds: number): string {
  const scale = ComputePrefixExponent(nanoseconds, 1000, durationUnits);
  const unitVal = nanoseconds / Math.pow(1000, scale);
  return unitVal.toFixed(1) + " " + durationUnits[scale];
}

const metricFormat = d3.format(".4s");
const decimalFormat = d3.format(".4f");

/**
 *  Count creates a string representation for a count.
 *
 *  For numbers larger than 1, the tooltip displays fractional values with
 *  metric multiplicative prefixes (e.g. kilo, mega, giga). For numbers smaller
 *  than 1, we simply display the fractional value without converting to a
 *  fractional metric prefix; this is because the use of fractional metric
 *  prefixes (i.e. milli, micro, nano) have proved confusing to users.
 */
export function Count(n: number) {
  if (n < 1) {
    return decimalFormat(n);
  }
  return metricFormat(n);
}
