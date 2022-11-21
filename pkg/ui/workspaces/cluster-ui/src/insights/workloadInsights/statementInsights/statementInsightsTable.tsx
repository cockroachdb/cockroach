// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
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
import { InsightExecEnum, FlattenedStmtInsightEvent } from "src/insights";
import {
  InsightCell,
  insightsTableTitles,
  StatementDetailsLink,
} from "../util";
import { FlattenedStmtInsights } from "src/api";
import { Tooltip } from "@cockroachlabs/ui-components";
import { Link } from "react-router-dom";
import classNames from "classnames/bind";
import styles from "../util/workloadInsights.module.scss";
import { TimeScale } from "../../../timeScaleDropdown";

const cx = classNames.bind(styles);

interface StatementInsightsTable {
  data: FlattenedStmtInsights;
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  visibleColumns: ColumnDescriptor<FlattenedStmtInsightEvent>[];
}

export function makeStatementInsightsColumns(
  setTimeScale: (ts: TimeScale) => void,
): ColumnDescriptor<FlattenedStmtInsightEvent>[] {
  const execType = InsightExecEnum.STATEMENT;
  return [
    {
      name: "latestExecutionID",
      title: insightsTableTitles.latestExecutionID(execType),
      cell: (item: FlattenedStmtInsightEvent) => (
        <Link to={`/insights/statement/${item.statementExecutionID}`}>
          {String(item.statementExecutionID)}
        </Link>
      ),
      sort: (item: FlattenedStmtInsightEvent) => item.statementExecutionID,
      alwaysShow: true,
    },
    {
      name: "statementFingerprintID",
      title: insightsTableTitles.fingerprintID(execType),
      cell: (item: FlattenedStmtInsightEvent) =>
        StatementDetailsLink(item, setTimeScale),
      sort: (item: FlattenedStmtInsightEvent) => item.statementFingerprintID,
      showByDefault: true,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: FlattenedStmtInsightEvent) => (
        <Tooltip placement="bottom" content={item.query}>
          <span className={cx("queries-row")}>{limitText(item.query, 50)}</span>
        </Tooltip>
      ),
      sort: (item: FlattenedStmtInsightEvent) => item.query,
      showByDefault: true,
    },
    {
      name: "insights",
      title: insightsTableTitles.insights(execType),
      cell: (item: FlattenedStmtInsightEvent) =>
        item.insights ? item.insights.map(insight => InsightCell(insight)) : "",
      sort: (item: FlattenedStmtInsightEvent) =>
        item.insights
          ? item.insights.map(insight => insight.label).toString()
          : "",
      showByDefault: true,
    },
    {
      name: "startTime",
      title: insightsTableTitles.startTime(execType),
      cell: (item: FlattenedStmtInsightEvent) =>
        item.startTime?.format(DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT),
      sort: (item: FlattenedStmtInsightEvent) => item.startTime.unix(),
      showByDefault: true,
    },
    {
      name: "elapsedTime",
      title: insightsTableTitles.elapsedTime(execType),
      cell: (item: FlattenedStmtInsightEvent) =>
        Duration(item.elapsedTimeMillis * 1e6),
      sort: (item: FlattenedStmtInsightEvent) => item.elapsedTimeMillis,
      showByDefault: true,
    },
    {
      name: "userName",
      title: insightsTableTitles.username(execType),
      cell: (item: FlattenedStmtInsightEvent) => item.username,
      sort: (item: FlattenedStmtInsightEvent) => item.username,
      showByDefault: true,
    },
    {
      name: "applicationName",
      title: insightsTableTitles.applicationName(execType),
      cell: (item: FlattenedStmtInsightEvent) => item.application,
      sort: (item: FlattenedStmtInsightEvent) => item.application,
      showByDefault: true,
    },
    {
      name: "rowsProcessed",
      title: insightsTableTitles.rowsProcessed(execType),
      className: cx("statements-table__col-rows-read"),
      cell: (item: FlattenedStmtInsightEvent) =>
        `${Count(item.rowsRead)} Reads / ${Count(item.rowsRead)} Writes`,
      sort: (item: FlattenedStmtInsightEvent) =>
        item.rowsRead + item.rowsWritten,
      showByDefault: true,
    },
    {
      name: "retries",
      title: insightsTableTitles.numRetries(execType),
      cell: (item: FlattenedStmtInsightEvent) => item.retries,
      sort: (item: FlattenedStmtInsightEvent) => item.retries,
      showByDefault: false,
    },
    {
      name: "contention",
      title: insightsTableTitles.contention(execType),
      cell: (item: FlattenedStmtInsightEvent) =>
        !item.totalContentionTime
          ? NO_SAMPLES_FOUND
          : Duration(item.totalContentionTime.asMilliseconds() * 1e6),
      sort: (item: FlattenedStmtInsightEvent) =>
        item.totalContentionTime?.asMilliseconds() ?? -1,
      showByDefault: false,
    },
    {
      name: "isFullScan",
      title: insightsTableTitles.isFullScan(execType),
      cell: (item: FlattenedStmtInsightEvent) => String(item.isFullScan),
      sort: (item: FlattenedStmtInsightEvent) => String(item.isFullScan),
      showByDefault: false,
    },
    {
      name: "transactionFingerprintID",
      title: insightsTableTitles.fingerprintID(InsightExecEnum.TRANSACTION),
      cell: (item: FlattenedStmtInsightEvent) => item.transactionFingerprintID,
      sort: (item: FlattenedStmtInsightEvent) => item.transactionFingerprintID,
      showByDefault: false,
    },
    {
      name: "transactionID",
      title: insightsTableTitles.latestExecutionID(InsightExecEnum.TRANSACTION),
      cell: (item: FlattenedStmtInsightEvent) => item.transactionExecutionID,
      sort: (item: FlattenedStmtInsightEvent) => item.transactionExecutionID,
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
