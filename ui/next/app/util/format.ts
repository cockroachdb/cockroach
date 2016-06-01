const kibi = 1024;
const units: string[] = ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];

interface UnitValue {
  value: number;
  units: string;
}

export function BytesToUnitValue(bytes: number): UnitValue {
  if (Math.abs(bytes) < kibi) {
    return {value: bytes, units: "B"};
  }
  let u = -1;
  do {
    bytes /= kibi;
    ++u;
  } while (Math.abs(bytes) >= kibi && u < units.length - 1);
  return {
    value: bytes,
    units: units[u],
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
