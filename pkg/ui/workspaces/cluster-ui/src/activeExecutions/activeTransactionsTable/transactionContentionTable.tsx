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
import { SortedTable, ColumnDescriptor } from "../../sortedtable";
import { ContendedTransaction } from "../types";
import { isSelectedColumn } from "../../columnsSelector/utils";
import { Link } from "react-router-dom";
import { StatusIcon } from "../statusIcon";
import {
  getLabel,
  executionsTableTitles,
  ExecutionType,
  ExecutionsColumn,
} from "../execTableCommon";
import { DATE_FORMAT, Duration } from "../../util";

export function makeTransactionContentionColumns(): ColumnDescriptor<ContendedTransaction>[] {
  const execType: ExecutionType = "transaction";
  const columns: ColumnDescriptor<ContendedTransaction>[] = [
    {
      name: "executionID",
      title: executionsTableTitles.executionID(execType),
      cell: item => (
        <Link to={`/execution/transaction/${item.executionID}`}>
          {item.executionID}
        </Link>
      ),
      sort: item => item.executionID,
      alwaysShow: true,
    },
    {
      name: "mostRecentStatement",
      title: executionsTableTitles.mostRecentStatement(execType),
      cell: item => (
        <Link
          to={`/execution/statement/${item.mostRecentStatement?.executionID}`}
        >
          {item.mostRecentStatement?.query}
        </Link>
      ),
      sort: item => item.mostRecentStatement?.query,
    },
    {
      name: "status",
      title: executionsTableTitles.status(execType),
      cell: item => (
        <span>
          <StatusIcon status={item.status} />
          {item.status}
        </span>
      ),
      sort: item => item.status,
    },
    {
      name: "startTime",
      title: executionsTableTitles.startTime(execType),
      cell: item => item.start.format(DATE_FORMAT),
      sort: item => item.start.unix(),
    },
    {
      name: "timeSpentBlocking",
      title: executionsTableTitles.timeSpentBlocking(execType),
      cell: item => Duration(item.timeSpentBlocking.asSeconds() * 1e9),
      sort: item => item.timeSpentBlocking.asSeconds(),
    },
  ];
  return columns;
}

export function getColumnOptions(
  selectedColumns: string[] | null,
): { label: string; value: string; isSelected: boolean }[] {
  return makeTransactionContentionColumns()
    .filter(col => !col.alwaysShow)
    .map(col => ({
      value: col.name,
      label: getLabel(col.name as ExecutionsColumn, "statement"),
      isSelected: isSelectedColumn(selectedColumns, col),
    }));
}

interface TransactionContentionTableProps {
  data: ContendedTransaction[];
  renderNoResult?: React.ReactNode;
}

const columns = makeTransactionContentionColumns();

export const TransactionContentionTable: React.FC<
  TransactionContentionTableProps
> = props => {
  return <SortedTable columns={columns} {...props} />;
};
