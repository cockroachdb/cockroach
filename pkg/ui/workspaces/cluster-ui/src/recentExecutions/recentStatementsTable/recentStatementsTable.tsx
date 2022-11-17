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
  recentStatementColumnsFromCommon,
  ExecutionsColumn,
  executionsTableTitles,
  getLabel,
} from "../execTableCommon";
import { RecentStatement } from "../types";
import { Tooltip } from "@cockroachlabs/ui-components";
import { limitText } from "../../util";

export function makeRecentStatementsColumns(
  isTenant: boolean,
): ColumnDescriptor<RecentStatement>[] {
  return [
    recentStatementColumnsFromCommon.executionID,
    {
      name: "execution",
      title: executionsTableTitles.execution("statement"),
      cell: (item: RecentStatement) => (
        <Tooltip placement="bottom" content={item.query}>
          <Link to={`/execution/statement/${item.statementID}`}>
            {limitText(item.query, 70)}
          </Link>
        </Tooltip>
      ),
      sort: (item: RecentStatement) => item.query,
    },
    recentStatementColumnsFromCommon.status,
    recentStatementColumnsFromCommon.startTime,
    recentStatementColumnsFromCommon.elapsedTime,
    !isTenant ? recentStatementColumnsFromCommon.timeSpentWaiting : null,
    recentStatementColumnsFromCommon.applicationName,
  ].filter(col => col != null);
}

export function getColumnOptions(
  columns: ColumnDescriptor<RecentStatement>[],
  selectedColumns: string[] | null,
): { label: string; value: string; isSelected: boolean }[] {
  return columns
    .filter(col => !col.alwaysShow)
    .map(col => ({
      value: col.name,
      label: getLabel(col.name as ExecutionsColumn, "statement"),
      isSelected: isSelectedColumn(selectedColumns, col),
    }));
}
