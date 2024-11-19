// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
 * Extend the TypeScript built-in type Pick to understand two levels of keys.
 * Useful for typing selectors that grab from a CachedDataReducer.
 */
export type Pick<T, K1 extends keyof T, K2 extends keyof T[K1]> = {
  [P1 in K1]: {
    [P2 in K2]: T[P1][P2];
  };
};
