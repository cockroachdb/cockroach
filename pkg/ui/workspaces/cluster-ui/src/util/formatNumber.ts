// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import isNumber from "lodash/isNumber";

function numberToString(n: number) {
  return n?.toString() || "";
}

export function formatNumberForDisplay(
  value: number,
  format: (n: number) => string = numberToString,
): string {
  if (!isNumber(value)) {
    return "-";
  }
  return format(value);
}
