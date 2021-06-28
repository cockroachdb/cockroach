// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { isNumber } from "lodash";

function numberToString(n: number) {
  return n.toString();
}

export function formatNumberForDisplay(
  value: number,
  format: (n: number) => string = numberToString,
) {
  if (!isNumber(value)) {
    return "-";
  }
  return format(value);
}
