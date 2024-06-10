// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Remove duplicates and return an array with unique elements.
export const unique = <T>(a: T[]): T[] => [...new Set([...a])];

// Check if array `a` contains any element of array `b`.
export const containAny = (a: string[], b: string[]): boolean => {
  return a.some(item => b.includes(item));
};
