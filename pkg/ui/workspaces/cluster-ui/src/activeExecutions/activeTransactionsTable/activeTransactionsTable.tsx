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
import { ActiveTransaction, ExecutionType } from "../types";
import { isSelectedColumn } from "../../columnsSelector/utils";
import { Link } from "react-router-dom";
import { StatusIcon } from "../statusIcon";
import {
  getLabel,
  executionsTableTitles,
  ExecutionsColumn,
  activeTransactionColumnsFromCommon,
} from "../execTableCommon";
import { DATE_FORMAT, Duration } from "../../util";

interface ActiveTransactionsTable {
  data: ActiveTransaction[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  selectedColumns: string[];
}

export function makeActiveTransactionsColumns(): ColumnDescriptor<ActiveTransaction>[] {
  const execType: ExecutionType = "transaction";
  const columns: ColumnDescriptor<ActiveTransaction>[] = [
    activeTransactionColumnsFromCommon.executionID,
    {
      name: "mostRecentStatement",
      title: executionsTableTitles.mostRecentStatement(execType),
      cell: (item: ActiveTransaction) => (
        <Link to={`/execution/statement/${item.statementID}`}>
          {item.query}
        </Link>
      ),
      sort: (item: ActiveTransaction) => item.query,
    },
    activeTransactionColumnsFromCommon.status,
    activeTransactionColumnsFromCommon.startTime,
    activeTransactionColumnsFromCommon.elapsedTime,
    activeTransactionColumnsFromCommon.timeSpentWaiting,
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
      label: getLabel(col.name as ExecutionsColumn, "transaction"),
      isSelected: isSelectedColumn(selectedColumns, col),
    }));
}

export const ActiveTransactionsTable: React.FC<
  ActiveTransactionsTable
> = props => {
  const { selectedColumns, ...rest } = props;
  const columns = makeActiveTransactionsColumns().filter(col =>
    isSelectedColumn(selectedColumns, col),
  );

  return (
    <SortedTable columns={columns} className="statements-table" {...rest} />
  );
};

ActiveTransactionsTable.defaultProps = {};
