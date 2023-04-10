// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { duration } from "moment";
import { SqlStatsSortOptions, SqlStatsSortType } from "src/api/statementsApi";

export const limitOptions = [
  { value: 25, label: "25" },
  { value: 50, label: "50" },
  { value: 100, label: "100" },
  { value: 500, label: "500" },
];

export function getSortLabel(sort: SqlStatsSortType): string {
  switch (sort) {
    case SqlStatsSortOptions.SERVICE_LAT:
      return "Service Latency";
    case SqlStatsSortOptions.EXECUTION_COUNT:
      return "Execution Count";
    case SqlStatsSortOptions.CONTENTION_TIME:
      return "Contention Time";
    case SqlStatsSortOptions.PCT_RUNTIME:
      return "% Of All Runtime";
    default:
      return "";
  }
}

export const stmtRequestSortOptions = Object.values(SqlStatsSortOptions)
  .map(sortVal => ({
    value: sortVal as SqlStatsSortType,
    label: getSortLabel(sortVal as SqlStatsSortType),
  }))
  .sort((a, b) => {
    if (a.label < b.label) return -1;
    if (a.label > b.label) return 1;
    return 0;
  });

export const txnRequestSortOptions = stmtRequestSortOptions.filter(
  option => option.value !== SqlStatsSortOptions.PCT_RUNTIME,
);

export const STATS_LONG_LOADING_DURATION = duration(2, "s");
