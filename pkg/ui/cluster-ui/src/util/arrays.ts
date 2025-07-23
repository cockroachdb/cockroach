import Long from "long";
import { longToInt } from "./fixLong";

// Remove duplicates and return an array with unique elements.
export const unique = <T>(a: T[]): T[] => [...new Set([...a])];

// Remove duplicates of an array of Long, returning an array with
// unique elements. Since Long is an object, `Set` cannot identify
// unique elements, so the array needs to be converted before
// creating the Set and converted back to be returned.
export const uniqueLong = (a: Long[]): Long[] => {
  return unique(a.map(longToInt)).map(n => Long.fromInt(n));
};

// Check if array `a` contains any element of array `b`.
export const containAny = (a: string[], b: string[]): boolean => {
  return a.some(item => b.includes(item));
};
