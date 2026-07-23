// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// `switchExhaustiveCheck` function ensures that all switch cases are defined,
// and default case will never occur.
export const switchExhaustiveCheck = (value: never): never => {
  throw new Error(`Unreachable point with value: ${value} `);
};
