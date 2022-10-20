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
import { Count, DATE_FORMAT, Duration, limitText } from "src/util";
import { InsightExecEnum, StatementInsightEvent } from "src/insights";
import {
  InsightCell,
  insightsTableTitles,
  StatementDetailsLink,
} from "../util";
import { StatementInsights } from "../../../api";
import { Tooltip } from "@cockroachlabs/ui-components";
import { Link } from "react-router-dom";
import classNames from "classnames/bind";
import styles from "../util/workloadInsights.module.scss";
import { TimeScale } from "../../../timeScaleDropdown";

const cx = classNames.bind(styles);

interface StatementInsightsTable {
  data: StatementInsights;
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  visibleColumns: ColumnDescriptor<StatementInsightEvent>[];
}

export function makeStatementInsightsColumns(
  setTimeScale: (ts: TimeScale) => void,
): ColumnDescriptor<StatementInsightEvent>[] {
  const execType = InsightExecEnum.STATEMENT;
  return [
    {
      name: "latestExecutionID",
      title: insightsTableTitles.latestExecutionID(execType),
      cell: (item: StatementInsightEvent) => (
        <Link to={`/insights/statement/${item.statementID}`}>
          {String(item.statementID)}
        </Link>
      ),
      sort: (item: StatementInsightEvent) => item.statementID,
      alwaysShow: true,
    },
    {
      name: "statementFingerprintID",
      title: insightsTableTitles.fingerprintID(execType),
      cell: (item: StatementInsightEvent) =>
        StatementDetailsLink(item, setTimeScale),
      sort: (item: StatementInsightEvent) => item.statementFingerprintID,
      showByDefault: true,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: StatementInsightEvent) => (
        <Tooltip placement="bottom" content={item.query}>
          <span className={cx("queries-row")}>{limitText(item.query, 50)}</span>
        </Tooltip>
      ),
      sort: (item: StatementInsightEvent) => item.query,
      showByDefault: true,
    },
    {
      name: "insights",
      title: insightsTableTitles.insights(execType),
      cell: (item: StatementInsightEvent) =>
        item.insights ? item.insights.map(insight => InsightCell(insight)) : "",
      sort: (item: StatementInsightEvent) =>
        item.insights
          ? item.insights.map(insight => insight.label).toString()
          : "",
      showByDefault: true,
    },
    {
      name: "startTime",
      title: insightsTableTitles.startTime(execType),
      cell: (item: StatementInsightEvent) => item.startTime.format(DATE_FORMAT),
      sort: (item: StatementInsightEvent) => item.startTime.unix(),
      showByDefault: true,
    },
    {
      name: "elapsedTime",
      title: insightsTableTitles.elapsedTime(execType),
      cell: (item: StatementInsightEvent) =>
        Duration(item.elapsedTimeMillis * 1e6),
      sort: (item: StatementInsightEvent) => item.elapsedTimeMillis,
      showByDefault: true,
    },
    {
      name: "userName",
      title: insightsTableTitles.username(execType),
      cell: (item: StatementInsightEvent) => item.username,
      sort: (item: StatementInsightEvent) => item.username,
      showByDefault: true,
    },
    {
      name: "applicationName",
      title: insightsTableTitles.applicationName(execType),
      cell: (item: StatementInsightEvent) => item.application,
      sort: (item: StatementInsightEvent) => item.application,
      showByDefault: true,
    },
    {
      name: "rowsProcessed",
      title: insightsTableTitles.rowsProcessed(execType),
      className: cx("statements-table__col-rows-read"),
      cell: (item: StatementInsightEvent) =>
        `${Count(item.rowsRead)} Reads / ${Count(item.rowsRead)} Writes`,
      sort: (item: StatementInsightEvent) => item.rowsRead + item.rowsWritten,
      showByDefault: true,
    },
    {
      name: "retries",
      title: insightsTableTitles.numRetries(execType),
      cell: (item: StatementInsightEvent) => item.retries,
      sort: (item: StatementInsightEvent) => item.retries,
      showByDefault: false,
    },
    {
      name: "contention",
      title: insightsTableTitles.contention(execType),
      cell: (item: StatementInsightEvent) =>
        !item.timeSpentWaiting
          ? "no samples"
          : Duration(item.timeSpentWaiting.asMilliseconds() * 1e6),
      sort: (item: StatementInsightEvent) =>
        item.timeSpentWaiting?.asMilliseconds() ?? -1,
      showByDefault: false,
    },
    {
      name: "isFullScan",
      title: insightsTableTitles.isFullScan(execType),
      cell: (item: StatementInsightEvent) => String(item.isFullScan),
      sort: (item: StatementInsightEvent) => String(item.isFullScan),
      showByDefault: false,
    },
    {
      name: "transactionFingerprintID",
      title: insightsTableTitles.fingerprintID(InsightExecEnum.TRANSACTION),
      cell: (item: StatementInsightEvent) => item.transactionFingerprintID,
      sort: (item: StatementInsightEvent) => item.transactionFingerprintID,
      showByDefault: false,
    },
    {
      name: "transactionID",
      title: insightsTableTitles.latestExecutionID(InsightExecEnum.TRANSACTION),
      cell: (item: StatementInsightEvent) => item.transactionID,
      sort: (item: StatementInsightEvent) => item.transactionID,
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
