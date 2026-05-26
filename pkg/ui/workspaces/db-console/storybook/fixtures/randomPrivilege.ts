// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import _ from "lodash";

export function randomTablePrivilege(): string {
  return _.sample([
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
