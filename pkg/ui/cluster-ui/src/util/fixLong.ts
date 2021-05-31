// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import Long from "long";

// FixLong deals with the fact that a Long that doesn't exist in a proto is
// returned as a constant number 0. This converts those constants back into a
// Long of value 0 or returns the original Long if it already exists.
export function FixLong(value: Long | number): Long {
  if ((value as any) === 0) {
    return Long.fromInt(0);
  }
  return value as Long;
}

export const longToInt = (value: number | Long) => Number(FixLong(value));
