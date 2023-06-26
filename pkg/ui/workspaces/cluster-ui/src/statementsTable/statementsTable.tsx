// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";

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
import { DATE_FORMAT } from "src/util/format";
import {
  countBarChart,
  bytesReadBarChart,
  latencyBarChart,
  contentionBarChart,
  maxMemUsageBarChart,
  networkBytesBarChart,
  retryBarChart,
  workloadPctBarChart,
} from "src/barCharts";
import { ActivateDiagnosticsModalRef } from "src/statementsDiagnostics";
import {
  ColumnDescriptor,
  longListWithTooltip,
  SortedTable,
} from "src/sortedtable";

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { StatementTableCell } from "./statementsTableContent";
import {
  statisticsTableTitles,
  NodeNames,
  StatisticType,
} from "../statsTableUtil/statsTableUtil";
import { BarChartOptions } from "src/barCharts/barChartFactory";

type IStatementDiagnosticsReport =
  cockroach.server.serverpb.IStatementDiagnosticsReport;
type ICollectedStatementStatistics =
  cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
import styles from "./statementsTable.module.scss";
const cx = classNames.bind(styles);

function makeCommonColumns(
  statements: AggregateStatistics[],
  totalWorkload: number,
  nodeRegions: { [nodeId: string]: string },
  statType: StatisticType,
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
        return longToInt(d.stats.exec_stats?.count) == 0;
      },
    };

  const countBar = countBarChart(statements, defaultBarChartOptions);
  const bytesReadBar = bytesReadBarChart(statements, defaultBarChartOptions);
  const latencyBar = latencyBarChart(statements, defaultBarChartOptions);
  const contentionBar = contentionBarChart(
    statements,
    sampledExecStatsBarChartOptions,
  );
  const maxMemUsageBar = maxMemUsageBarChart(
    statements,
    sampledExecStatsBarChartOptions,
  );
  const networkBytesBar = networkBytesBarChart(
    statements,
    sampledExecStatsBarChartOptions,
  );
  const retryBar = retryBarChart(statements, defaultBarChartOptions);

  return [
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
      cell: (stmt: AggregateStatistics) => stmt.database,
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
    {
      name: "regionNodes",
      title: statisticsTableTitles.regionNodes(statType),
      className: cx("statements-table__col-regions"),
      cell: (stmt: AggregateStatistics) => {
        return longListWithTooltip(stmt.regionNodes.sort().join(", "), 50);
      },
      sort: (stmt: AggregateStatistics) => stmt.regionNodes.sort().join(", "),
      hideIfTenant: true,
    },
    {
      name: "lastExecTimestamp",
      title: statisticsTableTitles.lastExecTimestamp(statType),
      cell: (stmt: AggregateStatistics) =>
        TimestampToMoment(stmt.stats.last_exec_timestamp).format(DATE_FORMAT),
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
}

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
  diagnosticsReports?: cockroach.server.serverpb.IStatementDiagnosticsReport[];
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

export function makeStatementFingerprintColumn(
  statType: StatisticType,
  selectedApps: string[],
  search?: string,
  onStatementClick?: (statement: string) => void,
): ColumnDescriptor<AggregateStatistics> {
  return {
    name: "statements",
    title: statisticsTableTitles.statements(statType),
    className: cx("cl-table__col-query-text"),
    cell: StatementTableCell.statements(search, selectedApps, onStatementClick),
    sort: stmt => stmt.label,
    alwaysShow: true,
  };
}

export function makeStatementsColumns(
  statements: AggregateStatistics[],
  selectedApps: string[],
  // totalWorkload is the sum of service latency of all statements listed on the table.
  totalWorkload: number,
  nodeRegions: { [nodeId: string]: string },
  statType: StatisticType,
  isTenant: boolean,
  hasViewActivityRedactedRole: boolean,
  search?: string,
  activateDiagnosticsRef?: React.RefObject<ActivateDiagnosticsModalRef>,
  onSelectDiagnosticsReportDropdownOption?: (
    report: IStatementDiagnosticsReport,
  ) => void,
  onStatementClick?: (statement: string) => void,
): ColumnDescriptor<AggregateStatistics>[] {
  const columns: ColumnDescriptor<AggregateStatistics>[] = [
    makeStatementFingerprintColumn(
      statType,
      selectedApps,
      search,
      onStatementClick,
    ),
  ];
  columns.push(
    ...makeCommonColumns(statements, totalWorkload, nodeRegions, statType),
  );

  if (activateDiagnosticsRef && !isTenant && !hasViewActivityRedactedRole) {
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

/**
 * For each statement, generate the list of regions and nodes it was
 * executed on. Each node is assigned to only one region and a region can
 * have multiple nodes.
 * E.g. of one element of the list: `gcp-us-east1 (n1, n2, n3)`
 * @param statements: list of statements containing details about which
 * node it was executed on.
 * @param nodeRegions: object with keys being the node id and the value
 * which region it belongs to.
 * @param isTenant: boolean indicating if the cluster is tenant, since
 * node information doesn't need to be populated on this case.
 */
export function populateRegionNodeForStatements(
  statements: AggregateStatistics[],
  nodeRegions: { [p: string]: string },
  isTenant: boolean,
): void {
  statements.forEach(stmt => {
    if (isTenant) {
      stmt.regionNodes = [];
      return;
    }
    const regions: { [region: string]: Set<number> } = {};
    // For each region, populate a list of all nodes where the statement was executed.
    // E.g. {"gcp-us-east1" : [1,3,4]}
    if (stmt.stats.nodes) {
      stmt.stats.nodes.forEach(node => {
        if (Object.keys(regions).includes(nodeRegions[node.toString()])) {
          regions[nodeRegions[node.toString()]].add(longToInt(node));
        } else {
          regions[nodeRegions[node.toString()]] = new Set([longToInt(node)]);
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
