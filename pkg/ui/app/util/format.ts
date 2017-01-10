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

export function BytesToUnitValue(bytes: number): UnitValue {
  let scale = ComputePrefixExponent(bytes, kibi, byteUnits);
  return {
    value: bytes / Math.pow(kibi, scale),
    units: byteUnits[scale],
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
  let unitVal = BytesToUnitValue(bytes);
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
  let scale = ComputePrefixExponent(nanoseconds, 1000, durationUnits);
  let unitVal = nanoseconds / Math.pow(1000, scale);
  return unitVal.toFixed(1) + " " + durationUnits[scale];
}
