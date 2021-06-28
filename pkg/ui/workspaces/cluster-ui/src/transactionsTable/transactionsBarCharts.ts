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
import { stdDevLong, Duration, Bytes, longToInt } from "src/util";
import classNames from "classnames/bind";
import styles from "../barCharts/barCharts.module.scss";
import { barChartFactory } from "src/barCharts/barChartFactory";
import { bar, approximify } from "src/barCharts/utils";

type Transaction = protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
const cx = classNames.bind(styles);

const countBar = [
  bar("count-first-try", (d: Transaction) =>
    longToInt(d.stats_data.stats.count),
  ),
];
const rowsReadBar = [
  bar("rows-read", (d: Transaction) =>
    longToInt(d.stats_data.stats.rows_read.mean),
  ),
];
const rowsReadStdDev = bar(cx("rows-read-dev"), (d: Transaction) =>
  stdDevLong(d.stats_data.stats.rows_read, d.stats_data.stats.count),
);
const bytesReadBar = [
  bar("bytes-read", (d: Transaction) =>
    longToInt(d.stats_data.stats.bytes_read.mean),
  ),
];
const bytesReadStdDev = bar(cx("bytes-read-dev"), (d: Transaction) =>
  stdDevLong(d.stats_data.stats.bytes_read, d.stats_data.stats.count),
);
const latencyBar = [
  bar(
    "bar-chart__service-lat",
    (d: Transaction) => d.stats_data.stats.service_lat.mean,
  ),
];
const latencyStdDev = bar(cx("bar-chart__overall-dev"), (d: Transaction) =>
  stdDevLong(d.stats_data.stats.service_lat, d.stats_data.stats.count),
);
const contentionBar = [
  bar(
    "contention",
    (d: Transaction) => d.stats_data.stats.exec_stats.contention_time?.mean,
  ),
];
const contentionStdDev = bar(cx("contention-dev"), (d: Transaction) =>
  stdDevLong(
    d.stats_data.stats.exec_stats.contention_time,
    d.stats_data.stats.exec_stats.count,
  ),
);
const maxMemUsageBar = [
  bar("max-mem-usage", (d: Transaction) =>
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
  bar("network-bytes", (d: Transaction) =>
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
export const transactionsRowsReadBarChart = barChartFactory(
  "grey",
  rowsReadBar,
  approximify,
  rowsReadStdDev,
);
export const transactionsBytesReadBarChart = barChartFactory(
  "grey",
  bytesReadBar,
  Bytes,
  bytesReadStdDev,
);
export const transactionsLatencyBarChart = barChartFactory(
  "grey",
  latencyBar,
  v => Duration(v * 1e9),
  latencyStdDev,
);
export const transactionsContentionBarChart = barChartFactory(
  "grey",
  contentionBar,
  v => Duration(v * 1e9),
  contentionStdDev,
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
