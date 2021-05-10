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
import { longToInt } from "./fixLong";

// Remove duplicates and return an array with unique elements.
export const unique = <T>(a: T[]): T[] => [...new Set([...a])];

// Remove duplicates of an array of Long, returning an array with
// unique elements. Since Long is an object, `Set` cannot identify
// unique elements, so the array needs to be converted before
// creating the Set and converted back to be returned.
export const uniqueLong = (a: Long[]): Long[] => {
  return unique(a.map(longToInt)).map((n) => Long.fromInt(n));
};
