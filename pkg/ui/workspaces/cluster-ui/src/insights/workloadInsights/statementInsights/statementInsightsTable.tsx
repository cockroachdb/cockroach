// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React from "react";
import { Link } from "react-router-dom";

import { Badge } from "src/badge";
import {
  InsightExecEnum,
  StatementStatus,
  StmtInsightEvent,
} from "src/insights";
import {
  ColumnDescriptor,
  ISortedTablePagination,
  SortedTable,
  SortSetting,
} from "src/sortedtable";
import {
  Count,
  DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT,
  Duration,
  limitText,
  NO_SAMPLES_FOUND,
} from "src/util";

import { Timestamp } from "../../../timestamp";
import {
  InsightCell,
  insightsTableTitles,
  StatementDetailsLink,
} from "../util";
import styles from "../util/workloadInsights.module.scss";

const cx = classNames.bind(styles);

function stmtStatusToString(status: StatementStatus) {
  switch (status) {
    case StatementStatus.COMPLETED:
      return "success";
    case StatementStatus.FAILED:
      return "danger";
    default:
      return "info";
  }
}

interface StatementInsightsTable {
  data: StmtInsightEvent[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  visibleColumns: ColumnDescriptor<StmtInsightEvent>[];
}

export function makeStatementInsightsColumns(): ColumnDescriptor<StmtInsightEvent>[] {
  const execType = InsightExecEnum.STATEMENT;
  return [
    {
      name: "latestExecutionID",
      title: insightsTableTitles.latestExecutionID(execType),
      cell: (item: StmtInsightEvent) => (
        <Link to={`/insights/statement/${item.statementExecutionID}`}>
          {String(item.statementExecutionID)}
        </Link>
      ),
      sort: (item: StmtInsightEvent) => item.statementExecutionID,
      alwaysShow: true,
    },
    {
      name: "statementFingerprintID",
      title: insightsTableTitles.fingerprintID(execType),
      cell: (item: StmtInsightEvent) => StatementDetailsLink(item),
      sort: (item: StmtInsightEvent) => item.statementFingerprintID,
      showByDefault: true,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: StmtInsightEvent) => (
        <Tooltip placement="bottom" content={item.query}>
          <span className={cx("queries-row")}>{limitText(item.query, 50)}</span>
        </Tooltip>
      ),
      sort: (item: StmtInsightEvent) => item.query,
      showByDefault: true,
    },
    {
      name: "status",
      title: insightsTableTitles.status(execType),
      cell: (item: StmtInsightEvent) => (
        <Badge text={item.status} status={stmtStatusToString(item.status)} />
      ),
      sort: (item: StmtInsightEvent) => item.status,
      showByDefault: true,
    },
    {
      name: "insights",
      title: insightsTableTitles.insights(execType),
      cell: (item: StmtInsightEvent) =>
        item.insights ? item.insights.map(insight => InsightCell(insight)) : "",
      sort: (item: StmtInsightEvent) =>
        item.insights
          ? item.insights.map(insight => insight.label).toString()
          : "",
      showByDefault: true,
    },
    {
      name: "startTime",
      title: insightsTableTitles.startTime(execType),
      cell: (item: StmtInsightEvent) =>
        item.startTime ? (
          <Timestamp
            time={item.startTime}
            format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT}
          />
        ) : (
          <>N/A</>
        ),
      sort: (item: StmtInsightEvent) => item.startTime.unix(),
      showByDefault: true,
    },
    {
      name: "elapsedTime",
      title: insightsTableTitles.elapsedTime(execType),
      cell: (item: StmtInsightEvent) => Duration(item.elapsedTimeMillis * 1e6),
      sort: (item: StmtInsightEvent) => item.elapsedTimeMillis,
      showByDefault: true,
    },
    {
      name: "userName",
      title: insightsTableTitles.username(execType),
      cell: (item: StmtInsightEvent) => item.username,
      sort: (item: StmtInsightEvent) => item.username,
      showByDefault: true,
    },
    {
      name: "applicationName",
      title: insightsTableTitles.applicationName(execType),
      cell: (item: StmtInsightEvent) => item.application,
      sort: (item: StmtInsightEvent) => item.application,
      showByDefault: true,
    },
    {
      name: "rowsProcessed",
      title: insightsTableTitles.rowsProcessed(execType),
      className: cx("statements-table__col-rows-read"),
      cell: (item: StmtInsightEvent) =>
        `${Count(item.rowsRead)} Reads / ${Count(item.rowsWritten)} Writes`,
      sort: (item: StmtInsightEvent) => item.rowsRead + item.rowsWritten,
      showByDefault: true,
    },
    {
      name: "retries",
      title: insightsTableTitles.numRetries(execType),
      cell: (item: StmtInsightEvent) => item.retries,
      sort: (item: StmtInsightEvent) => item.retries,
      showByDefault: false,
    },
    {
      name: "contention",
      title: insightsTableTitles.contention(execType),
      cell: (item: StmtInsightEvent) =>
        !item.contentionTime
          ? NO_SAMPLES_FOUND
          : Duration(item.contentionTime.asMilliseconds() * 1e6),
      sort: (item: StmtInsightEvent) =>
        item.contentionTime?.asMilliseconds() ?? -1,
      showByDefault: false,
    },
    {
      name: "cpu",
      title: insightsTableTitles.cpu(execType),
      cell: (item: StmtInsightEvent) => Duration(item.cpuSQLNanos),
      sort: (item: StmtInsightEvent) => item.cpuSQLNanos,
      showByDefault: false,
    },
    {
      name: "isFullScan",
      title: insightsTableTitles.isFullScan(execType),
      cell: (item: StmtInsightEvent) => String(item.isFullScan),
      sort: (item: StmtInsightEvent) => String(item.isFullScan),
      showByDefault: false,
    },
    {
      name: "transactionFingerprintID",
      title: insightsTableTitles.fingerprintID(InsightExecEnum.TRANSACTION),
      cell: (item: StmtInsightEvent) => item.transactionFingerprintID,
      sort: (item: StmtInsightEvent) => item.transactionFingerprintID,
      showByDefault: false,
    },
    {
      name: "transactionExecutionID",
      title: insightsTableTitles.latestExecutionID(InsightExecEnum.TRANSACTION),
      cell: (item: StmtInsightEvent) => item.transactionExecutionID,
      sort: (item: StmtInsightEvent) => item.transactionExecutionID,
      showByDefault: false,
    },
  ];
}

export const StatementInsightsTable: React.FC<
  StatementInsightsTable
> = props => {
  return (
    <SortedTable
      columns={props.visibleColumns}
      className="statements-table"
      {...props}
    />
  );
};

StatementInsightsTable.defaultProps = {};
