// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// e.g. intersperse(["foo", "bar", "baz"], "-") => ["foo", "-", "bar", "-", "baz"]
export function intersperse<T>(array: T[], sep: T): T[] {
  const output = [];
  for (let i = 0; i < array.length; i++) {
    if (i > 0) {
      output.push(sep);
    }
    output.push(array[i]);
  }
  return output;
}
