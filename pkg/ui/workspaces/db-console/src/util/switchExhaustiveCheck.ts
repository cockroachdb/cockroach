// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// `switchExhaustiveCheck` function ensures that all switch cases are defined,
// and default case will never occur.
export const switchExhaustiveCheck = (value: never): never => {
  throw new Error(`Unreachable point with value: ${value} `);
};
