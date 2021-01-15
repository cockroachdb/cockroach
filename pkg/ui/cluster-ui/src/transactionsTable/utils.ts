import { FixLong } from "src/util";

export const longToInt = (value: number | Long) => Number(FixLong(value));

export const limitText = (text: string, limit: number): string => {
  return text.length > limit ? text.slice(0, limit - 3).concat("...") : text;
};
