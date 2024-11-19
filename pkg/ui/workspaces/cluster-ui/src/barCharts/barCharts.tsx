// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";

import { AggregateStatistics } from "src/statementsTable/statementsTable";
import { stdDevLong, longToInt } from "src/util";
import { Duration, Bytes, PercentageCustom } from "src/util/format";

import { barChartFactory, BarChartOptions } from "./barChartFactory";
import styles from "./barCharts.module.scss";
import { bar, approximify } from "./utils";

type StatementStatistics =
  protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
const cx = classNames.bind(styles);

const countBars = [
  bar("count-first-try", (d: StatementStatistics) =>
    longToInt(d.stats.first_attempt_count),
  ),
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

const cpuBars = [
  bar(
    "cpu",
    (d: StatementStatistics) => d.stats.exec_stats.cpu_sql_nanos?.mean,
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

const bytesReadStdDev = bar(cx("bytes-read-dev"), (d: StatementStatistics) =>
  stdDevLong(d.stats.bytes_read, d.stats.count),
);
const latencyStdDev = bar(
  cx("bar-chart__overall-dev"),
  (d: StatementStatistics) => stdDevLong(d.stats.service_lat, d.stats.count),
);
const contentionStdDev = bar(cx("contention-dev"), (d: StatementStatistics) =>
  stdDevLong(d.stats.exec_stats.contention_time, d.stats.exec_stats.count),
);
const cpuStdDev = bar(cx("cpu-dev"), (d: StatementStatistics) =>
  stdDevLong(d.stats.exec_stats.cpu_sql_nanos, d.stats.exec_stats.count),
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
export const cpuBarChart = barChartFactory(
  "grey",
  cpuBars,
  v => Duration(v),
  cpuStdDev,
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
  defaultBarChartOptions: BarChartOptions<object>,
  totalWorkload: number,
) {
  return barChartFactory(
    "grey",
    [
      bar("pct-workload", (d: StatementStatistics) =>
        totalWorkload !== 0
          ? (d.stats.service_lat.mean * longToInt(d.stats.count)) /
            totalWorkload
          : 0,
      ),
    ],
    v => PercentageCustom(v, 1, 1),
  )(statements, defaultBarChartOptions);
}
