// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";
import React from "react";

import {
  countBarChart,
  bytesReadBarChart,
  latencyBarChart,
  contentionBarChart,
  cpuBarChart,
  maxMemUsageBarChart,
  networkBytesBarChart,
  retryBarChart,
  workloadPctBarChart,
} from "src/barCharts";
import { BarChartOptions } from "src/barCharts/barChartFactory";
import {
  ColumnDescriptor,
  longListWithTooltip,
  SortedTable,
} from "src/sortedtable";
import { ActivateDiagnosticsModalRef } from "src/statementsDiagnostics";
import {
  FixLong,
  longToInt,
  StatementSummary,
  StatementStatistics,
  Count,
  TimestampToNumber,
  TimestampToMoment,
  unset,
} from "src/util";
import { DATE_FORMAT, Duration } from "src/util/format";

import { StatementDiagnosticsReport } from "../api";
import {
  statisticsTableTitles,
  StatisticType,
} from "../statsTableUtil/statsTableUtil";
import { Timestamp } from "../timestamp";

import styles from "./statementsTable.module.scss";
import { StatementTableCell } from "./statementsTableContent";

type ICollectedStatementStatistics =
  cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

const cx = classNames.bind(styles);

export interface AggregateStatistics {
  aggregatedFingerprintID: string;
  aggregatedFingerprintHexID: string;
  // label is either shortStatement (StatementsPage) or nodeId (StatementDetails).
  label: string;
  // summary exists only for SELECT/INSERT/UPSERT/UPDATE statements, and is
  // replaced with shortStatement otherwise.
  summary: string;
  aggregatedTs: number;
  implicitTxn: boolean;
  fullScan: boolean;
  database: string;
  applicationName: string;
  stats: StatementStatistics;
  drawer?: boolean;
  firstCellBordered?: boolean;
  diagnosticsReports?: StatementDiagnosticsReport[];
  // totalWorkload is the sum of service latency of all statements listed on the table.
  totalWorkload?: Long;
  regionNodes?: string[];
}

export class StatementsSortedTable extends SortedTable<AggregateStatistics> {}

export function shortStatement(
  summary: StatementSummary,
  original: string,
): string {
  switch (summary.statement) {
    case "update":
      return "UPDATE " + summary.table;
    case "insert":
      return "INSERT INTO " + summary.table;
    case "select":
      return "SELECT FROM " + summary.table;
    case "delete":
      return "DELETE FROM " + summary.table;
    case "create":
      return "CREATE TABLE " + summary.table;
    case "set":
      return "SET " + summary.table;
    default:
      return original;
  }
}

function formatStringArray(databases: string): string {
  try {
    // Case where the database is returned as an array in a string form.
    const d = JSON.parse(databases);
    try {
      // Case where the database is returned as an array of array in a string form.
      return JSON.parse(d).join(", ");
    } catch (e) {
      return d.join(", ");
    }
  } catch (e) {
    // Case where the database is a single value as a string.
    return databases;
  }
}

export function makeStatementsColumns(
  statements: AggregateStatistics[],
  selectedApps: string[],
  // totalWorkload is the sum of service latency of all statements listed on the table.
  totalWorkload: number,
  statType: StatisticType,
  isTenant: boolean,
  hasViewActivityRedactedRole: boolean,
  search?: string,
  activateDiagnosticsRef?: React.RefObject<ActivateDiagnosticsModalRef>,
  onSelectDiagnosticsReportDropdownOption?: (
    report: StatementDiagnosticsReport,
  ) => void,
  onStatementClick?: (statement: string) => void,
): ColumnDescriptor<AggregateStatistics>[] {
  const defaultBarChartOptions = {
    classes: {
      root: cx("statements-table__col--bar-chart"),
      label: cx("statements-table__col--bar-chart__label"),
    },
  };

  const sampledExecStatsBarChartOptions: BarChartOptions<ICollectedStatementStatistics> =
    {
      classes: defaultBarChartOptions.classes,
      displayNoSamples: (d: ICollectedStatementStatistics) => {
        return longToInt(d.stats.exec_stats?.count) === 0;
      },
    };

  const countBar = countBarChart(statements, defaultBarChartOptions);
  const bytesReadBar = bytesReadBarChart(statements, defaultBarChartOptions);
  const latencyBar = latencyBarChart(statements, defaultBarChartOptions);
  const contentionBar = contentionBarChart(
    statements,
    sampledExecStatsBarChartOptions,
  );
  const cpuBar = cpuBarChart(statements, sampledExecStatsBarChartOptions);
  const maxMemUsageBar = maxMemUsageBarChart(
    statements,
    sampledExecStatsBarChartOptions,
  );
  const networkBytesBar = networkBytesBarChart(
    statements,
    sampledExecStatsBarChartOptions,
  );
  const retryBar = retryBarChart(statements, defaultBarChartOptions);

  const columns: ColumnDescriptor<AggregateStatistics>[] = [
    {
      name: "statements",
      title: statisticsTableTitles.statements(statType),
      className: cx("cl-table__col-query-text"),
      cell: StatementTableCell.statements(
        search,
        selectedApps,
        onStatementClick,
      ),
      sort: stmt => stmt.label,
      alwaysShow: true,
    },
    {
      name: "executionCount",
      title: statisticsTableTitles.executionCount(statType),
      className: cx("statements-table__col-count"),
      cell: countBar,
      sort: (stmt: AggregateStatistics) => FixLong(Number(stmt.stats.count)),
    },
    {
      name: "database",
      title: statisticsTableTitles.database(statType),
      className: cx("statements-table__col-database"),
      cell: (stmt: AggregateStatistics) => formatStringArray(stmt.database),
      sort: (stmt: AggregateStatistics) => stmt.database,
      showByDefault: false,
    },
    {
      name: "applicationName",
      title: statisticsTableTitles.applicationName(statType),
      className: cx("statements-table__col-app-name"),
      cell: (stmt: AggregateStatistics) =>
        stmt.applicationName?.length > 0 ? stmt.applicationName : unset,
      sort: (stmt: AggregateStatistics) => stmt.applicationName,
    },
    {
      name: "time",
      title: statisticsTableTitles.time(statType),
      className: cx("statements-table__col-latency"),
      cell: latencyBar,
      sort: (stmt: AggregateStatistics) => stmt.stats.service_lat.mean,
    },
    {
      name: "workloadPct",
      title: statisticsTableTitles.workloadPct(statType),
      cell: workloadPctBarChart(
        statements,
        defaultBarChartOptions,
        totalWorkload,
      ),
      sort: (stmt: AggregateStatistics) =>
        (stmt.stats.service_lat.mean * longToInt(stmt.stats.count)) /
        totalWorkload,
    },
    {
      name: "contention",
      title: statisticsTableTitles.contention(statType),
      cell: contentionBar,
      sort: (stmt: AggregateStatistics) =>
        FixLong(Number(stmt.stats.exec_stats.contention_time.mean)),
    },
    {
      name: "cpu",
      title: statisticsTableTitles.cpu(statType),
      cell: cpuBar,
      sort: (stmt: AggregateStatistics) =>
        FixLong(Number(stmt.stats.exec_stats.cpu_sql_nanos?.mean)),
    },
    {
      name: "latencyMin",
      title: statisticsTableTitles.latencyMin(statType),
      cell: (stmt: AggregateStatistics) =>
        Duration(stmt.stats.latency_info?.min * 1e9),
      sort: (stmt: AggregateStatistics) =>
        FixLong(Number(stmt.stats.latency_info?.min)),
      showByDefault: false,
    },
    {
      name: "latencyMax",
      title: statisticsTableTitles.latencyMax(statType),
      cell: (stmt: AggregateStatistics) =>
        Duration(stmt.stats.latency_info?.max * 1e9),
      sort: (stmt: AggregateStatistics) =>
        FixLong(Number(stmt.stats.latency_info?.max)),
    },
    {
      name: "rowsProcessed",
      title: statisticsTableTitles.rowsProcessed(statType),
      className: cx("statements-table__col-rows-read"),
      cell: (stmt: AggregateStatistics) =>
        `${Count(Number(stmt.stats.rows_read.mean))} Reads / ${Count(
          Number(stmt.stats.rows_written?.mean),
        )} Writes`,
      sort: (stmt: AggregateStatistics) =>
        FixLong(
          Number(stmt.stats.rows_read.mean) +
            Number(stmt.stats.rows_written?.mean),
        ),
    },
    {
      name: "bytesRead",
      title: statisticsTableTitles.bytesRead(statType),
      cell: bytesReadBar,
      sort: (stmt: AggregateStatistics) =>
        FixLong(Number(stmt.stats.bytes_read.mean)),
    },
    {
      name: "maxMemUsage",
      title: statisticsTableTitles.maxMemUsage(statType),
      cell: maxMemUsageBar,
      sort: (stmt: AggregateStatistics) =>
        FixLong(Number(stmt.stats.exec_stats.max_mem_usage.mean)),
    },
    {
      name: "networkBytes",
      title: statisticsTableTitles.networkBytes(statType),
      cell: networkBytesBar,
      sort: (stmt: AggregateStatistics) =>
        FixLong(Number(stmt.stats.exec_stats.network_bytes.mean)),
    },
    {
      name: "retries",
      title: statisticsTableTitles.retries(statType),
      className: cx("statements-table__col-retries"),
      cell: retryBar,
      sort: (stmt: AggregateStatistics) =>
        longToInt(stmt.stats.count) - longToInt(stmt.stats.first_attempt_count),
    },
    makeRegionsColumn(statType, isTenant),
    {
      name: "lastExecTimestamp",
      title: statisticsTableTitles.lastExecTimestamp(statType),
      cell: (stmt: AggregateStatistics) => (
        <Timestamp
          time={TimestampToMoment(stmt.stats.last_exec_timestamp)}
          format={DATE_FORMAT}
        />
      ),
      sort: (stmt: AggregateStatistics) =>
        TimestampToNumber(stmt.stats.last_exec_timestamp),
      showByDefault: false,
    },
    {
      name: "statementFingerprintId",
      title: statisticsTableTitles.statementFingerprintId(statType),
      cell: (stmt: AggregateStatistics) => stmt.aggregatedFingerprintHexID,
      sort: (stmt: AggregateStatistics) => stmt.aggregatedFingerprintHexID,
      showByDefault: false,
    },
  ];

  if (activateDiagnosticsRef && !hasViewActivityRedactedRole) {
    const diagnosticsColumn: ColumnDescriptor<AggregateStatistics> = {
      name: "diagnostics",
      title: statisticsTableTitles.diagnostics(statType),
      cell: StatementTableCell.diagnostics(
        activateDiagnosticsRef,
        onSelectDiagnosticsReportDropdownOption,
      ),
      sort: stmt => {
        if (stmt.diagnosticsReports?.length > 0) {
          // Perform sorting by first diagnostics report as only
          // this one can be either in ready or waiting status.
          return stmt.diagnosticsReports[0].completed ? "READY" : "WAITING";
        }
        return null;
      },
      alwaysShow: true,
      titleAlign: "right",
    };
    columns.push(diagnosticsColumn);
  }
  return columns;
}

function makeRegionsColumn(
  statType: StatisticType,
  isTenant: boolean,
): ColumnDescriptor<AggregateStatistics> {
  if (isTenant) {
    return {
      name: "regions",
      title: statisticsTableTitles.regions(statType),
      className: cx("statements-table__col-regions"),
      cell: (stmt: AggregateStatistics) => {
        return longListWithTooltip(stmt.stats.regions.sort().join(", "), 50);
      },
      sort: (stmt: AggregateStatistics) => stmt.stats.regions.sort().join(", "),
    };
  } else {
    return {
      name: "regionNodes",
      title: statisticsTableTitles.regionNodes(statType),
      className: cx("statements-table__col-regions"),
      cell: (stmt: AggregateStatistics) => {
        return longListWithTooltip(stmt.regionNodes.sort().join(", "), 50);
      },
      sort: (stmt: AggregateStatistics) => stmt.regionNodes.sort().join(", "),
    };
  }
}

/**
 * For each statement, generate the list of regions and nodes it was
 * executed on. Each node is assigned to only one region and a region can
 * have multiple nodes.
 * E.g. of one element of the list: `gcp-us-east1 (n1, n2, n3)`
 * @param statements: list of statements containing details about which
 * node it was executed on.
 * @param nodeRegions: object with keys being the node id and the value
 * which region it belongs to.
 */
export function populateRegionNodeForStatements(
  statements: AggregateStatistics[],
  nodeRegions: { [p: string]: string },
): void {
  statements.forEach(stmt => {
    const regions: { [region: string]: Set<number> } = {};
    // For each region, populate a list of all nodes where the statement was executed.
    // E.g. {"gcp-us-east1" : [1,3,4]}
    if (stmt.stats.nodes) {
      stmt.stats.nodes.forEach(node => {
        const region = nodeRegions[node.toString()];
        if (region) {
          if (Object.keys(regions).includes(region)) {
            regions[region].add(longToInt(node));
          } else {
            regions[region] = new Set([longToInt(node)]);
          }
        }
      });
    }
    // Create a list nodes/regions where a statement was executed on, with
    // format: region (node1,node2)
    const regionNodes: string[] = [];
    Object.keys(regions).forEach(region => {
      regionNodes.push(
        region +
          " (" +
          Array.from(regions[region])
            .sort()
            .map(n => "n" + n)
            .toString() +
          ")",
      );
    });
    stmt.regionNodes = regionNodes;
  });
}
