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
import classNames from "classnames/bind";
import {
  ActiveStatement,
  ActiveStatementFilters,
  calculateActiveFilters,
} from "src";
import ColumnsSelector, {
  SelectOption,
} from "src/columnsSelector/columnsSelector";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { EmptyStatementsPlaceholder } from "src/statementsPage/emptyStatementsPlaceholder";
import { TableStatistics } from "src/tableStatistics";
import { useQueryParmeters } from "../hooks/useQueryParameters";
import {
  ISortedTablePagination,
  SortSetting,
} from "../sortedtable/sortedtable";
import {
  ActiveStatementsTable,
  getColumnOptions,
} from "./activeStatementsTable";
import { StatementViewType } from "src/statementsPage/statementPageTypes";
import { useTableSortFromURL } from "src/sortedtable/useTableSortFromURL";

const sortableTableCx = classNames.bind(sortableTableStyles);

const SEARCH_QUERY_PARAM = "q";

type ActiveStatementsSectionProps = {
  filters: ActiveStatementFilters;
  pagination: ISortedTablePagination;
  search: string;
  statements: ActiveStatement[];
  selectedColumns?: string[];
  onChangeSortSetting: (sortSetting: SortSetting) => void;
  onClearFilters: () => void;
  onColumnsSelect: (columns: string[]) => void;
};

export const ActiveStatementsSection: React.FC<ActiveStatementsSectionProps> = ({
  filters,
  pagination,
  search,
  statements,
  selectedColumns,
  onClearFilters,
  onChangeSortSetting,
  onColumnsSelect,
}) => {
  const queryParams = useQueryParmeters();

  const tableColumns: SelectOption[] = getColumnOptions(selectedColumns);
  const data = statements || [];
  const sortSetting = useTableSortFromURL({
    ascending: true,
    columnTitle: "startTime",
  });
  const activeFilters = calculateActiveFilters(filters);

  return (
    <section className={sortableTableCx("cl-table-container")}>
      <div>
        <ColumnsSelector
          options={tableColumns}
          onSubmitColumns={onColumnsSelect}
        />
        <TableStatistics
          pagination={pagination}
          search={queryParams.get(SEARCH_QUERY_PARAM)}
          totalCount={data.length}
          arrayItemName="statements"
          activeFilters={activeFilters}
          onClearFilters={onClearFilters}
        />
      </div>
      <ActiveStatementsTable
        data={data}
        selectedColumns={selectedColumns}
        sortSetting={sortSetting}
        onChangeSortSetting={onChangeSortSetting}
        renderNoResult={
          <EmptyStatementsPlaceholder
            isEmptySearchResults={search?.length > 0 && data?.length > 0}
            statementView={StatementViewType.ACTIVE}
          />
        }
        pagination={pagination}
      />
    </section>
  );
};
