// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

import Long from "long";

// FixLong deals with the fact that a Long that doesn't exist in a proto is
// returned as a constant number 0. This converts those constants back into a
// Long of value 0 or returns the original Long if it already exists.
export function FixLong(value: Long | number): Long {
  if (value as any === 0) {
    return Long.fromInt(0);
  }
  return value as Long;
}
