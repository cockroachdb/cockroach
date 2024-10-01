// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
