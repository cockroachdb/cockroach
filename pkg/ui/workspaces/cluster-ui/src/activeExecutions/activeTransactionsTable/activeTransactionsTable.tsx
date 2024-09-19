// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import React from "react";
import { Link } from "react-router-dom";

import { isSelectedColumn } from "../../columnsSelector/utils";
import { ColumnDescriptor } from "../../sortedtable";
import { limitText } from "../../util";
import {
  activeTransactionColumnsFromCommon,
  ExecutionsColumn,
  executionsTableTitles,
  getLabel,
} from "../execTableCommon";
import { ActiveTransaction, ExecutionType } from "../types";

export function makeActiveTransactionsColumns(
  isTenant: boolean,
): ColumnDescriptor<ActiveTransaction>[] {
  const execType: ExecutionType = "transaction";
  return [
    activeTransactionColumnsFromCommon.executionID,
    {
      name: "mostRecentStatement",
      title: executionsTableTitles.mostRecentStatement(execType),
      cell: (item: ActiveTransaction) => {
        const queryText = limitText(item.query || "", 70);
        return (
          <Tooltip placement="bottom" content={item.query}>
            {item.statementID ? (
              <Link to={`/execution/statement/${item.statementID}`}>
                {queryText}
              </Link>
            ) : (
              queryText
            )}
          </Tooltip>
        );
      },
      sort: (item: ActiveTransaction) => item.query,
    },
    activeTransactionColumnsFromCommon.status,
    activeTransactionColumnsFromCommon.startTime,
    activeTransactionColumnsFromCommon.elapsedTime,
    !isTenant ? activeTransactionColumnsFromCommon.timeSpentWaiting : null,
    {
      name: "statementCount",
      title: executionsTableTitles.statementCount(execType),
      cell: (item: ActiveTransaction) => item.statementCount,
      sort: (item: ActiveTransaction) => item.statementCount,
    },
    {
      name: "retries",
      title: executionsTableTitles.retries(execType),
      cell: (item: ActiveTransaction) => item.retries,
      sort: (item: ActiveTransaction) => item.retries,
    },
    activeTransactionColumnsFromCommon.applicationName,
  ].filter(col => col != null);
}

export function getColumnOptions(
  columns: ColumnDescriptor<ActiveTransaction>[],
  selectedColumns: string[] | null,
): { label: string; value: string; isSelected: boolean }[] {
  return columns
    .filter(col => !col.alwaysShow)
    .map(col => ({
      value: col.name,
      label: getLabel(col.name as ExecutionsColumn, "transaction"),
      isSelected: isSelectedColumn(selectedColumns, col),
    }));
}
