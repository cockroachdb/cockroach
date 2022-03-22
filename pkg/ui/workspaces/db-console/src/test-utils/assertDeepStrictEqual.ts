// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import assert from "assert";

export function assertDeepStrictEqual<T>(expected: T, actual: T) {
  assert.deepStrictEqual(actual, expected, errorMessage(expected, actual));
}

function errorMessage(expected: any, actual: any): string {
  return `expected: ${JSON.stringify(expected)} but was ${JSON.stringify(
    actual,
  )}`;
}
