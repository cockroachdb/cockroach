// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import sample from "lodash/sample";

export function randomTablePrivilege(): string {
  return sample([
    "ALL",
    "CREATE",
    "DROP",
    "GRANT",
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
  ]);
}
