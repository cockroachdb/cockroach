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
import { ActiveStatement, ExecutionType } from "../types";
import { isSelectedColumn } from "../../columnsSelector/utils";
import {
  getLabel,
  ExecutionsColumn,
  activeStatementColumnsFromCommon,
  executionsTableTitles,
} from "../execTableCommon";
import { Link } from "react-router-dom";

interface ActiveStatementsTable {
  data: ActiveStatement[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  selectedColumns: string[];
}

export function makeActiveStatementsColumns(): ColumnDescriptor<ActiveStatement>[] {
  return [
    activeStatementColumnsFromCommon.executionID,
    {
      name: "execution",
      title: executionsTableTitles.execution("statement"),
      cell: (item: ActiveStatement) => (
        <Link to={`/execution/statement/${item.statementID}`}>
          {item.query}
        </Link>
      ),
      sort: (item: ActiveStatement) => item.query,
    },
    activeStatementColumnsFromCommon.status,
    activeStatementColumnsFromCommon.startTime,
    activeStatementColumnsFromCommon.elapsedTime,
    activeStatementColumnsFromCommon.timeSpentWaiting,
    activeStatementColumnsFromCommon.applicationName,
  ];
}

export function getColumnOptions(
  selectedColumns: string[] | null,
): { label: string; value: string; isSelected: boolean }[] {
  return makeActiveStatementsColumns()
    .filter(col => !col.alwaysShow)
    .map(col => ({
      value: col.name,
      label: getLabel(col.name as ExecutionsColumn, "statement"),
      isSelected: isSelectedColumn(selectedColumns, col),
    }));
}

export const ActiveStatementsTable: React.FC<ActiveStatementsTable> = props => {
  const { selectedColumns, ...rest } = props;
  const columns = makeActiveStatementsColumns().filter(col =>
    isSelectedColumn(selectedColumns, col),
  );

  return (
    <SortedTable columns={columns} className="statements-table" {...rest} />
  );
};

ActiveStatementsTable.defaultProps = {};
