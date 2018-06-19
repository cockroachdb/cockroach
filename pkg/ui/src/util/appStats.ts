import { FixLong } from "src/util/fixLong";

export interface NumericStat {
  mean?: number;
  squared_diffs?: number;
}

export function variance(stat: NumericStat, count: number) {
  return stat.squared_diffs / (count - 1);
}

export function stdDev(stat: NumericStat, count: number) {
  return Math.sqrt(variance(stat, count));
}

export function stdDevLong(stat: NumericStat, count: number | Long) {
  return stdDev(stat, FixLong(count).toInt());
}
