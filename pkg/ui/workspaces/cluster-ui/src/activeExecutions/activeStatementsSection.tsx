// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React, { useMemo } from "react";

import {
  ActiveStatement,
  ActiveStatementFilters,
} from "src/activeExecutions/types";
import ColumnsSelector, {
  SelectOption,
} from "src/columnsSelector/columnsSelector";
import { isSelectedColumn } from "src/columnsSelector/utils";
import { calculateActiveFilters } from "src/queryFilter/filter";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { EmptyStatementsPlaceholder } from "src/statementsPage/emptyStatementsPlaceholder";
import { StatementViewType } from "src/statementsPage/statementPageTypes";
import { TableStatistics } from "src/tableStatistics";

import {
  ISortedTablePagination,
  SortedTable,
  SortSetting,
} from "../sortedtable/sortedtable";

import {
  getColumnOptions,
  makeActiveStatementsColumns,
} from "./activeStatementsTable";

const sortableTableCx = classNames.bind(sortableTableStyles);

type ActiveStatementsSectionProps = {
  filters: ActiveStatementFilters;
  pagination: ISortedTablePagination;
  search: string;
  statements: ActiveStatement[];
  selectedColumns?: string[];
  sortSetting: SortSetting;
  isTenant?: boolean;
  onChangeSortSetting: (sortSetting: SortSetting) => void;
  onClearFilters: () => void;
  onColumnsSelect: (columns: string[]) => void;
};

export const ActiveStatementsSection: React.FC<
  ActiveStatementsSectionProps
> = ({
  filters,
  isTenant,
  pagination,
  search,
  statements,
  selectedColumns,
  sortSetting,
  onClearFilters,
  onChangeSortSetting,
  onColumnsSelect,
}) => {
  const columns = useMemo(
    () => makeActiveStatementsColumns(isTenant),
    [isTenant],
  );

  const shownColumns = columns.filter(col =>
    isSelectedColumn(selectedColumns, col),
  );

  const tableColumns: SelectOption[] = getColumnOptions(
    columns,
    selectedColumns,
  );
  const activeFilters = calculateActiveFilters(filters);

  return (
    <section className={sortableTableCx("cl-table-container")}>
      <div>
        <ColumnsSelector
          options={tableColumns}
          onSubmitColumns={onColumnsSelect}
          size={"small"}
        />
        <TableStatistics
          pagination={pagination}
          search={search}
          totalCount={statements.length}
          arrayItemName="statements"
          activeFilters={activeFilters}
          onClearFilters={onClearFilters}
        />
      </div>
      <SortedTable
        className="statements-table"
        data={statements}
        columns={shownColumns}
        sortSetting={sortSetting}
        onChangeSortSetting={onChangeSortSetting}
        renderNoResult={
          <EmptyStatementsPlaceholder
            isEmptySearchResults={
              (search?.length > 0 || activeFilters > 0) &&
              statements.length === 0
            }
            statementView={StatementViewType.ACTIVE}
          />
        }
        pagination={pagination}
      />
    </section>
  );
};
