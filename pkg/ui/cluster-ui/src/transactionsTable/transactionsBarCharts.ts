import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { stdDevLong } from "src/util";
import { Duration } from "src/util/format";
import classNames from "classnames/bind";
import styles from "../barCharts/barCharts.module.scss";
import { barChartFactory } from "src/barCharts/barChartFactory";
import {
  bar,
  formatTwoPlaces,
  longToInt,
  approximify,
} from "src/barCharts/utils";

type Transaction = protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
const cx = classNames.bind(styles);

const retryBar = [
  bar("count-retry", (d: Transaction) =>
    longToInt(d.stats_data.stats.max_retries),
  ),
];
const countBar = [
  bar("count-first-try", (d: Transaction) =>
    longToInt(d.stats_data.stats.count),
  ),
];
const latencyBar = [
  bar(
    "bar-chart__service-lat",
    (d: Transaction) => d.stats_data.stats.service_lat.mean,
  ),
];
const latencyStdDev = bar(cx("bar-chart__overall-dev"), (d: Transaction) =>
  stdDevLong(d.stats_data.stats.service_lat, d.stats_data.stats.count),
);
const rowsBar = [
  bar("rows", (d: Transaction) => d.stats_data.stats.num_rows.mean),
];
const rowsStdDev = bar(cx("rows-dev"), (d: Transaction) =>
  stdDevLong(d.stats_data.stats.num_rows, d.stats_data.stats.count),
);

export const transactionsCountBarChart = barChartFactory(
  "grey",
  countBar,
  approximify,
);
export const transactionsRetryBarChart = barChartFactory(
  "red",
  retryBar,
  approximify,
);
export const transactionsRowsBarChart = barChartFactory(
  "grey",
  rowsBar,
  approximify,
  rowsStdDev,
  formatTwoPlaces,
);
export const transactionsLatencyBarChart = barChartFactory(
  "grey",
  latencyBar,
  v => Duration(v * 1e9),
  latencyStdDev,
);
