// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo } from "react";
import classNames from "classnames/bind";
import {
  RecentStatement,
  RecentStatementFilters,
} from "src/recentExecutions/types";
import ColumnsSelector, {
  SelectOption,
} from "src/columnsSelector/columnsSelector";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { EmptyStatementsPlaceholder } from "src/statementsPage/emptyStatementsPlaceholder";
import { TableStatistics } from "src/tableStatistics";
import {
  ISortedTablePagination,
  SortedTable,
  SortSetting,
} from "../sortedtable/sortedtable";
import {
  getColumnOptions,
  makeRecentStatementsColumns,
} from "./recentStatementsTable";
import { StatementViewType } from "src/statementsPage/statementPageTypes";
import { calculateActiveFilters } from "src/queryFilter/filter";
import { isSelectedColumn } from "src/columnsSelector/utils";

const sortableTableCx = classNames.bind(sortableTableStyles);

type RecentStatementsSectionProps = {
  filters: RecentStatementFilters;
  pagination: ISortedTablePagination;
  search: string;
  statements: RecentStatement[];
  selectedColumns?: string[];
  sortSetting: SortSetting;
  isTenant?: boolean;
  onChangeSortSetting: (sortSetting: SortSetting) => void;
  onClearFilters: () => void;
  onColumnsSelect: (columns: string[]) => void;
};

export const RecentStatementsSection: React.FC<
  RecentStatementsSectionProps
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
    () => makeRecentStatementsColumns(isTenant),
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
