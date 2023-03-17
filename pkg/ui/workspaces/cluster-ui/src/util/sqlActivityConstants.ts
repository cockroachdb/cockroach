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
import { TimeScale } from "../timeScaleDropdown";

export const limitOptions = [
  { label: "25", value: 25 },
  { label: "50", value: 50 },
  { label: "100", value: 100 },
  { label: "500", value: 500 },
];

export enum SqlStatsSortOptions {
  CONTENTION_TIME = "CONTENTION_TIME",
  CPU_TIME = "CPU_TIME",
  EXECUTION_COUNT = "EXECUTION_COUNT",
  PCT_RUNTIME = "PCT_RUNTIME",
  SERVICE_LAT = "SERVICE_LAT",
}
export function getSortLabel(sort: SqlStatsSortOptions): string {
  switch (sort) {
    case SqlStatsSortOptions.CONTENTION_TIME:
      return "Contention Time";
    case SqlStatsSortOptions.CPU_TIME:
      return "CPU Time";
    case SqlStatsSortOptions.EXECUTION_COUNT:
      return "Execution Count";
    case SqlStatsSortOptions.PCT_RUNTIME:
      return "% of Runtime";
    case SqlStatsSortOptions.SERVICE_LAT:
      return "Service Latency";
    default:
      return "";
  }
}

export const requestSortOptions = Object.values(SqlStatsSortOptions).map(
  sortVal => ({
    value: sortVal,
    label: getSortLabel(sortVal as SqlStatsSortOptions),
  }),
);

export type StatsRequestParams = {
  timeScale: TimeScale;
  limit: number;
};

export const STATS_LONG_LOADING_DURATION = duration(2, "s");
