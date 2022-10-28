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
import { Link } from "react-router-dom";
import { isSelectedColumn } from "../../columnsSelector/utils";
import { ColumnDescriptor } from "../../sortedtable";
import {
  recentTransactionColumnsFromCommon,
  ExecutionsColumn,
  executionsTableTitles,
  getLabel,
} from "../execTableCommon";
import { RecentTransaction, ExecutionType } from "../types";
import { Tooltip } from "@cockroachlabs/ui-components";
import { limitText } from "../../util";

export function makeRecentTransactionsColumns(
  isTenant: boolean,
): ColumnDescriptor<RecentTransaction>[] {
  const execType: ExecutionType = "transaction";
  return [
    recentTransactionColumnsFromCommon.executionID,
    {
      name: "mostRecentStatement",
      title: executionsTableTitles.mostRecentStatement(execType),
      cell: (item: RecentTransaction) => {
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
      sort: (item: RecentTransaction) => item.query,
    },
    recentTransactionColumnsFromCommon.status,
    recentTransactionColumnsFromCommon.startTime,
    recentTransactionColumnsFromCommon.elapsedTime,
    !isTenant ? recentTransactionColumnsFromCommon.timeSpentWaiting : null,
    {
      name: "statementCount",
      title: executionsTableTitles.statementCount(execType),
      cell: (item: RecentTransaction) => item.statementCount,
      sort: (item: RecentTransaction) => item.statementCount,
    },
    {
      name: "retries",
      title: executionsTableTitles.retries(execType),
      cell: (item: RecentTransaction) => item.retries,
      sort: (item: RecentTransaction) => item.retries,
    },
    recentTransactionColumnsFromCommon.applicationName,
  ].filter(col => col != null);
}

export function getColumnOptions(
  columns: ColumnDescriptor<RecentTransaction>[],
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
