// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
