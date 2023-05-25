// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { duration } from "moment";
import { SqlStatsSortOptions, SqlStatsSortType } from "src/api/statementsApi";
import {
  getLabel,
  StatisticTableColumnKeys,
} from "../statsTableUtil/statsTableUtil";
import classNames from "classnames/bind";
import styles from "src/sqlActivity/sqlActivity.module.scss";

const cx = classNames.bind(styles);

export const limitOptions = [
  { value: 25, label: "25" },
  { value: 50, label: "50" },
  { value: 100, label: "100" },
  { value: 500, label: "500" },
  { value: 1000, label: "1000" },
  { value: 5000, label: "5000" },
  { value: 10000, label: "10000" },
];

function isSortOptionForStatementOnly(sort: SqlStatsSortType): boolean {
  switch (sort) {
    case SqlStatsSortOptions.PCT_RUNTIME:
    case SqlStatsSortOptions.LAST_EXEC:
      return true;
    default:
      return false;
  }
}

export function getSortLabel(
  sort: SqlStatsSortType,
  type: "Statement" | "Transaction",
): string {
  switch (sort) {
    case SqlStatsSortOptions.SERVICE_LAT:
      return `${type} Time`;
    case SqlStatsSortOptions.EXECUTION_COUNT:
      return "Execution Count";
    case SqlStatsSortOptions.CONTENTION_TIME:
      return "Contention Time";
    case SqlStatsSortOptions.PCT_RUNTIME:
      return "% of All Runtime";
    case SqlStatsSortOptions.ROWS_PROCESSED:
      return "Rows Processed";
    case SqlStatsSortOptions.MAX_MEMORY:
      return "Max Memory";
    case SqlStatsSortOptions.NETWORK:
      return "Network";
    case SqlStatsSortOptions.RETRIES:
      return "Retries";
    case SqlStatsSortOptions.LAST_EXEC:
      return "Last Execution Time";
    default:
      return "";
  }
}

export function getSortColumn(sort: SqlStatsSortType): string {
  switch (sort) {
    case SqlStatsSortOptions.SERVICE_LAT:
      return "time";
    case SqlStatsSortOptions.EXECUTION_COUNT:
      return "executionCount";
    case SqlStatsSortOptions.CONTENTION_TIME:
      return "contention";
    case SqlStatsSortOptions.PCT_RUNTIME:
      return "workloadPct";
    case SqlStatsSortOptions.ROWS_PROCESSED:
      return "rowsProcessed";
    case SqlStatsSortOptions.MAX_MEMORY:
      return "maxMemUsage";
    case SqlStatsSortOptions.NETWORK:
      return "networkBytes";
    case SqlStatsSortOptions.RETRIES:
      return "retries";
    case SqlStatsSortOptions.LAST_EXEC:
      return "lastExecTimestamp";
    default:
      return "";
  }
}

export function getReqSortColumn(sort: string): SqlStatsSortType {
  switch (sort) {
    case "time":
      return SqlStatsSortOptions.SERVICE_LAT;
    case "executionCount":
      return SqlStatsSortOptions.EXECUTION_COUNT;
    case "contention":
      return SqlStatsSortOptions.CONTENTION_TIME;
    case "workloadPct":
      return SqlStatsSortOptions.PCT_RUNTIME;
    case "rowsProcessed":
      return SqlStatsSortOptions.ROWS_PROCESSED;
    case "maxMemUsage":
      return SqlStatsSortOptions.MAX_MEMORY;
    case "networkBytes":
      return SqlStatsSortOptions.NETWORK;
    case "retries":
      return SqlStatsSortOptions.RETRIES;
    case "lastExecTimestamp":
      return SqlStatsSortOptions.LAST_EXEC;
    default:
      return SqlStatsSortOptions.SERVICE_LAT;
  }
}

export const stmtRequestSortOptions = Object.values(SqlStatsSortOptions)
  .map(sortVal => ({
    value: sortVal as SqlStatsSortType,
    label: getSortLabel(sortVal as SqlStatsSortType, "Statement"),
  }))
  .sort((a, b) => {
    if (a.label < b.label) return -1;
    if (a.label > b.label) return 1;
    return 0;
  });

export const txnRequestSortOptions = Object.values(SqlStatsSortOptions)
  .filter(sort => !isSortOptionForStatementOnly(sort as SqlStatsSortType))
  .map(sortVal => ({
    value: sortVal as SqlStatsSortType,
    label: getSortLabel(sortVal as SqlStatsSortType, "Transaction"),
  }))
  .sort((a, b) => {
    if (a.label < b.label) return -1;
    if (a.label > b.label) return 1;
    return 0;
  });

export const STATS_LONG_LOADING_DURATION = duration(2, "s");

export function getSubsetWarning(
  type: "statement" | "transaction",
  limit: number,
  sortLabel: string,
  columnTitle: StatisticTableColumnKeys,
  onUpdateSortSettingAndApply: () => void,
): React.ReactElement {
  return (
    <span className={cx("row")}>
      {`You are viewing a subset (Top ${limit}) of fingerprints by ${sortLabel}.`}
      &nbsp;
      <a onClick={onUpdateSortSettingAndApply} className={cx("action")}>
        Update the search criteria
      </a>
      &nbsp;
      {`to see the ${type} fingerprints sorted on ${getLabel(
        columnTitle,
        type,
      )}.`}
    </span>
  );
}
