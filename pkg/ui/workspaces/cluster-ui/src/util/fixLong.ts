// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import Long from "long";

// FixLong deals with the fact that a Long that doesn't exist in a proto is
// returned as a constant number 0. This converts those constants back into a
// Long of value 0 or returns the original Long if it already exists.
export function FixLong(value: Long | number): Long {
  if ((value as unknown) === 0) {
    return Long.fromInt(0);
  }
  return value as Long;
}

export const longToInt = (value: number | Long): number =>
  Number(FixLong(value));
