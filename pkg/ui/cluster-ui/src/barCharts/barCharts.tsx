import React from "react";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { stdDevLong } from "src/util";
import { Duration } from "src/util/format";
import classNames from "classnames/bind";
import styles from "./barCharts.module.scss";
import { bar, formatTwoPlaces, longToInt, approximify } from "./utils";
import { barChartFactory } from "./barChartFactory";

type StatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
const cx = classNames.bind(styles);

const countBars = [
  bar("count-first-try", (d: StatementStatistics) =>
    longToInt(d.stats.first_attempt_count),
  ),
];

const retryBars = [
  bar(
    "count-retry",
    (d: StatementStatistics) =>
      longToInt(d.stats.count) - longToInt(d.stats.first_attempt_count),
  ),
];

const rowsBars = [
  bar("rows", (d: StatementStatistics) => d.stats.num_rows.mean),
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

const latencyStdDev = bar(
  cx("bar-chart__overall-dev"),
  (d: StatementStatistics) => stdDevLong(d.stats.service_lat, d.stats.count),
);
const rowsStdDev = bar(cx("rows-dev"), (d: StatementStatistics) =>
  stdDevLong(d.stats.num_rows, d.stats.count),
);

export const countBarChart = barChartFactory("grey", countBars, approximify);
export const retryBarChart = barChartFactory("red", retryBars, approximify);
export const rowsBarChart = barChartFactory(
  "grey",
  rowsBars,
  approximify,
  rowsStdDev,
  formatTwoPlaces,
);
export const latencyBarChart = barChartFactory(
  "grey",
  latencyBars,
  v => Duration(v * 1e9),
  latencyStdDev,
);
