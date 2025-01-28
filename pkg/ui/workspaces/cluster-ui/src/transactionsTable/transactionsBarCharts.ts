// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";

import { barChartFactory } from "src/barCharts/barChartFactory";
import { bar, approximify } from "src/barCharts/utils";
import { stdDevLong, Duration, Bytes, longToInt } from "src/util";

import styles from "../barCharts/barCharts.module.scss";

import { TransactionInfo } from "./transactionsTable";

type Transaction =
  protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
const cx = classNames.bind(styles);

const countBar = [
  bar("count-first-try", (d: Transaction) =>
    longToInt(d.stats_data.stats.count),
  ),
];
const bytesReadBar = [
  bar("bytes-read", (d: Transaction) =>
    longToInt(d.stats_data.stats.bytes_read.mean),
  ),
];
const bytesReadStdDev = bar(cx("bytes-read-dev"), (d: Transaction) =>
  stdDevLong(d.stats_data.stats.bytes_read, d.stats_data.stats.count),
);
const serviceLatencyBar = [
  bar(
    "bar-chart__service-lat",
    (d: Transaction) => d.stats_data.stats.service_lat.mean,
  ),
];
const serviceLatencyStdDev = bar(
  cx("bar-chart__overall-dev"),
  (d: Transaction) =>
    stdDevLong(d.stats_data.stats.service_lat, d.stats_data.stats.count),
);
const commitLatencyBar = [
  bar(
    "bar-chart__commit-lat",
    (d: Transaction) => d.stats_data.stats.commit_lat.mean,
  ),
];
const commitLatencyStdDev = bar("bar-chart__commit-dev", (d: Transaction) =>
  stdDevLong(d.stats_data.stats.commit_lat, d.stats_data.stats.count),
);
const contentionBar = [
  bar(
    "contention",
    (d: TransactionInfo) => d.stats_data.stats.exec_stats.contention_time?.mean,
  ),
];
const contentionStdDev = bar(cx("contention-dev"), (d: Transaction) =>
  stdDevLong(
    d.stats_data.stats.exec_stats.contention_time,
    d.stats_data.stats.exec_stats.count,
  ),
);
const cpuBar = [
  bar(
    "cpu",
    (d: TransactionInfo) => d.stats_data.stats.exec_stats.cpu_sql_nanos?.mean,
  ),
];
const cpuStdDev = bar(cx("cpu-dev"), (d: Transaction) =>
  stdDevLong(
    d.stats_data.stats.exec_stats.cpu_sql_nanos,
    d.stats_data.stats.exec_stats.count,
  ),
);
const maxMemUsageBar = [
  bar("max-mem-usage", (d: TransactionInfo) =>
    longToInt(d.stats_data.stats.exec_stats.max_mem_usage?.mean),
  ),
];
const maxMemUsageStdDev = bar(cx("max-mem-usage-dev"), (d: Transaction) =>
  stdDevLong(
    d.stats_data.stats.exec_stats.max_mem_usage,
    d.stats_data.stats.exec_stats.count,
  ),
);
const networkBytesBar = [
  bar("network-bytes", (d: TransactionInfo) =>
    longToInt(d.stats_data.stats.exec_stats.network_bytes?.mean),
  ),
];
const networkBytesStdDev = bar(cx("network-bytes-dev"), (d: Transaction) =>
  stdDevLong(
    d.stats_data.stats.exec_stats.network_bytes,
    d.stats_data.stats.exec_stats.count,
  ),
);
const retryBar = [
  bar("count-retry", (d: Transaction) =>
    longToInt(d.stats_data.stats.max_retries),
  ),
];

export const transactionsCountBarChart = barChartFactory(
  "grey",
  countBar,
  approximify,
);
export const transactionsBytesReadBarChart = barChartFactory(
  "grey",
  bytesReadBar,
  Bytes,
  bytesReadStdDev,
);
export const transactionsServiceLatencyBarChart = barChartFactory(
  "grey",
  serviceLatencyBar,
  v => Duration(v * 1e9),
  serviceLatencyStdDev,
);
export const transactionsCommitLatencyBarChart = barChartFactory(
  "grey",
  commitLatencyBar,
  v => Duration(v * 1e9),
  commitLatencyStdDev,
);
export const transactionsContentionBarChart = barChartFactory(
  "grey",
  contentionBar,
  v => Duration(v * 1e9),
  contentionStdDev,
);
export const transactionsCPUBarChart = barChartFactory(
  "grey",
  cpuBar,
  v => Duration(v),
  cpuStdDev,
);
export const transactionsMaxMemUsageBarChart = barChartFactory(
  "grey",
  maxMemUsageBar,
  Bytes,
  maxMemUsageStdDev,
);
export const transactionsNetworkBytesBarChart = barChartFactory(
  "grey",
  networkBytesBar,
  Bytes,
  networkBytesStdDev,
);
export const transactionsRetryBarChart = barChartFactory(
  "red",
  retryBar,
  approximify,
);
