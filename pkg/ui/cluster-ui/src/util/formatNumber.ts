import { isNumber } from "lodash";

function numberToString(n: number) {
  return n.toString();
}

export function formatNumberForDisplay(
  value: number,
  format: (n: number) => string = numberToString,
) {
  if (!isNumber(value)) {
    return "-";
  }
  return format(value);
}
