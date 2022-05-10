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
import { ActiveStatement } from "../types";
import { isSelectedColumn } from "../../columnsSelector/utils";
import { Link } from "react-router-dom";
import { StatusIcon } from "../statusIcon";
import {
  getLabel,
  executionsTableTitles,
  ExecutionType,
  ExecutionsColumn,
} from "../execTableCommon";

interface ActiveStatementsTableProps {
  data: ActiveStatement[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  selectedColumns: string[];
}

export function makeActiveStatementsColumns(): ColumnDescriptor<
  ActiveStatement
>[] {
  const execType: ExecutionType = "statement";
  const columns: ColumnDescriptor<ActiveStatement>[] = [
    {
      name: "executionID",
      title: executionsTableTitles.executionID(execType),
      cell: (item: ActiveStatement) => (
        <Link to={`/execution/statement/${item.executionID}`}>
          {item.executionID}
        </Link>
      ),
      sort: (item: ActiveStatement) => item.executionID,
      alwaysShow: true,
    },
    {
      name: "execution",
      title: executionsTableTitles.execution(execType),
      cell: (item: ActiveStatement) => (
        <Link to={`/execution/statement/${item.executionID}`}>
          {item.query}
        </Link>
      ),
      sort: (item: ActiveStatement) => item.query,
    },
    {
      name: "status",
      title: executionsTableTitles.status(execType),
      cell: (item: ActiveStatement) => (
        <span>
          <StatusIcon status={item.status} />
          {item.status}
        </span>
      ),
      sort: (item: ActiveStatement) => item.status,
    },
    {
      name: "startTime",
      title: executionsTableTitles.startTime(execType),
      cell: (item: ActiveStatement) =>
        item.start.format("MMM D, YYYY [at] h:mm a"),
      sort: (item: ActiveStatement) => item.start.unix(),
    },
    {
      name: "elapsedTime",
      title: executionsTableTitles.elapsedTime(execType),
      cell: (item: ActiveStatement) => `${item.elapsedTimeSeconds} s`,
      sort: (item: ActiveStatement) => item.elapsedTimeSeconds,
    },
    {
      name: "applicationName",
      title: executionsTableTitles.applicationName(execType),
      cell: (item: ActiveStatement) => item.application,
      sort: (item: ActiveStatement) => item.application,
    },
  ];
  return columns;
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

export const ActiveStatementsTable: React.FC<ActiveStatementsTableProps> = props => {
  const { selectedColumns, ...rest } = props;
  const columns = makeActiveStatementsColumns().filter(col =>
    isSelectedColumn(selectedColumns, col),
  );

  return (
    <SortedTable columns={columns} className="statements-table" {...rest} />
  );
};

ActiveStatementsTable.defaultProps = {};
