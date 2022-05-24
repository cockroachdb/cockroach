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
  SortedTable,
  ISortedTablePagination,
  ColumnDescriptor,
} from "../../sortedtable";
import { SortSetting } from "../../sortedtable";
import { ActiveTransaction } from "../types";
import { isSelectedColumn } from "../../columnsSelector/utils";
import { Link } from "react-router-dom";
import { StatusIcon } from "../statusIcon";
import {
  getLabel,
  executionsTableTitles,
  ExecutionType,
  ExecutionsColumn,
} from "../execTableCommon";

interface ActiveTransactionsTableProps {
  data: ActiveTransaction[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  selectedColumns: string[];
}

export function makeActiveTransactionsColumns(): ColumnDescriptor<
  ActiveTransaction
>[] {
  const execType: ExecutionType = "transaction";
  const columns: ColumnDescriptor<ActiveTransaction>[] = [
    {
      name: "executionID",
      title: executionsTableTitles.executionID(execType),
      cell: (item: ActiveTransaction) => (
        <Link to={`/execution/transaction/${item.executionID}`}>
          {item.executionID}
        </Link>
      ),
      sort: (item: ActiveTransaction) => item.executionID,
      alwaysShow: true,
    },
    {
      name: "mostRecentStatement",
      title: executionsTableTitles.mostRecentStatement(execType),
      cell: (item: ActiveTransaction) => (
        <Link
          to={`/execution/statement/${item.mostRecentStatement?.executionID}`}
        >
          {item.mostRecentStatement?.query}
        </Link>
      ),
      sort: (item: ActiveTransaction) => item.mostRecentStatement?.query,
    },
    {
      name: "status",
      title: executionsTableTitles.status(execType),
      cell: (item: ActiveTransaction) => (
        <span>
          <StatusIcon status={item.status} />
          {item.status}
        </span>
      ),
      sort: (item: ActiveTransaction) => item.status,
    },
    {
      name: "startTime",
      title: executionsTableTitles.startTime(execType),
      cell: (item: ActiveTransaction) =>
        item.start.format("MMM D, YYYY [at] H:mm"),
      sort: (item: ActiveTransaction) => item.start.unix(),
    },
    {
      name: "elapsedTime",
      title: executionsTableTitles.elapsedTime(execType),
      cell: (item: ActiveTransaction) => `${item.elapsedTimeSeconds} s`,
      sort: (item: ActiveTransaction) => item.elapsedTimeSeconds,
    },
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
    {
      name: "applicationName",
      title: executionsTableTitles.applicationName(execType),
      cell: (item: ActiveTransaction) => item.application,
      sort: (item: ActiveTransaction) => item.application,
    },
  ];
  return columns;
}

export function getColumnOptions(
  selectedColumns: string[] | null,
): { label: string; value: string; isSelected: boolean }[] {
  return makeActiveTransactionsColumns()
    .filter(col => !col.alwaysShow)
    .map(col => ({
      value: col.name,
      label: getLabel(col.name as ExecutionsColumn, "statement"),
      isSelected: isSelectedColumn(selectedColumns, col),
    }));
}

export const ActiveTransactionsTable: React.FC<ActiveTransactionsTableProps> = props => {
  const { selectedColumns, ...rest } = props;
  const columns = makeActiveTransactionsColumns().filter(col =>
    isSelectedColumn(selectedColumns, col),
  );

  return (
    <SortedTable columns={columns} className="statements-table" {...rest} />
  );
};

ActiveTransactionsTable.defaultProps = {};
