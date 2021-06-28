// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { stdDevLong, longToInt } from "src/util";
import { Duration, Bytes, Percentage } from "src/util/format";
import classNames from "classnames/bind";
import styles from "./barCharts.module.scss";
import { bar, formatTwoPlaces, approximify } from "./utils";
import { barChartFactory, BarChartOptions } from "./barChartFactory";
import { AggregateStatistics } from "src/statementsTable/statementsTable";

type StatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
const cx = classNames.bind(styles);

const countBars = [
  bar("count-first-try", (d: StatementStatistics) =>
    longToInt(d.stats.first_attempt_count),
  ),
];

const rowsReadBars = [
  bar("rows-read", (d: StatementStatistics) => d.stats.rows_read.mean),
];

const bytesReadBars = [
  bar("bytes-read", (d: StatementStatistics) => d.stats.bytes_read.mean),
];

const latencyBars = [
  bar("bar-chart__parse", (d: StatementStatistics) => d.stats.parse_lat.mean),
  bar("bar-chart__plan", (d: StatementStatistics) => d.stats.plan_lat.mean),
  bar("bar-chart__run", (d: StatementStatistics) => d.stats.run_lat.mean),
  bar(
    "bar-chart__overhead",
    (d: StatementStatistics) => d.stats.overhead_lat.mean,
  ),
];

const contentionBars = [
  bar(
    "contention",
    (d: StatementStatistics) => d.stats.exec_stats.contention_time?.mean,
  ),
];

const maxMemUsageBars = [
  bar(
    "max-mem-usage",
    (d: StatementStatistics) => d.stats.exec_stats.max_mem_usage?.mean,
  ),
];

const networkBytesBars = [
  bar(
    "network-bytes",
    (d: StatementStatistics) => d.stats.exec_stats.network_bytes?.mean,
  ),
];

const retryBars = [
  bar(
    "count-retry",
    (d: StatementStatistics) =>
      longToInt(d.stats.count) - longToInt(d.stats.first_attempt_count),
  ),
];

const rowsReadStdDev = bar(cx("rows-read-dev"), (d: StatementStatistics) =>
  stdDevLong(d.stats.rows_read, d.stats.count),
);
const bytesReadStdDev = bar(cx("rows-read-dev"), (d: StatementStatistics) =>
  stdDevLong(d.stats.bytes_read, d.stats.count),
);
const latencyStdDev = bar(
  cx("bar-chart__overall-dev"),
  (d: StatementStatistics) => stdDevLong(d.stats.service_lat, d.stats.count),
);
const contentionStdDev = bar(cx("contention-dev"), (d: StatementStatistics) =>
  stdDevLong(d.stats.exec_stats.contention_time, d.stats.exec_stats.count),
);
const maxMemUsageStdDev = bar(
  cx("max-mem-usage-dev"),
  (d: StatementStatistics) =>
    stdDevLong(d.stats.exec_stats.max_mem_usage, d.stats.exec_stats.count),
);
const networkBytesStdDev = bar(
  cx("network-bytes-dev"),
  (d: StatementStatistics) =>
    stdDevLong(d.stats.exec_stats.network_bytes, d.stats.exec_stats.count),
);

export const countBarChart = barChartFactory("grey", countBars, approximify);
export const rowsReadBarChart = barChartFactory(
  "grey",
  rowsReadBars,
  approximify,
  rowsReadStdDev,
  formatTwoPlaces,
);
export const bytesReadBarChart = barChartFactory(
  "grey",
  bytesReadBars,
  Bytes,
  bytesReadStdDev,
);
export const latencyBarChart = barChartFactory(
  "grey",
  latencyBars,
  v => Duration(v * 1e9),
  latencyStdDev,
);
export const contentionBarChart = barChartFactory(
  "grey",
  contentionBars,
  v => Duration(v * 1e9),
  contentionStdDev,
);
export const maxMemUsageBarChart = barChartFactory(
  "grey",
  maxMemUsageBars,
  Bytes,
  maxMemUsageStdDev,
);
export const networkBytesBarChart = barChartFactory(
  "grey",
  networkBytesBars,
  Bytes,
  networkBytesStdDev,
);

export const retryBarChart = barChartFactory("red", retryBars, approximify);

export function workloadPctBarChart(
  statements: AggregateStatistics[],
  defaultBarChartOptions: BarChartOptions<any>,
  totalWorkload: number,
) {
  return barChartFactory(
    "grey",
    [
      bar(
        "pct-workload",
        (d: StatementStatistics) =>
          (d.stats.service_lat.mean * longToInt(d.stats.count)) / totalWorkload,
      ),
    ],
    v => Percentage(v, 1, 1),
  )(statements, defaultBarChartOptions);
}
