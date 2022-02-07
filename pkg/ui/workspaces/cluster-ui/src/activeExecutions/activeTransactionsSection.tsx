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
  ActiveStatementFilters,
  ActiveTransaction,
  calculateActiveFilters,
} from "src";
import ColumnsSelector, {
  SelectOption,
} from "src/columnsSelector/columnsSelector";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { EmptyTransactionsPlaceholder } from "src/transactionsPage/emptyTransactionsPlaceholder";
import { TableStatistics } from "src/tableStatistics";
import { useQueryParmeters } from "../hooks/useQueryParameters";
import {
  ISortedTablePagination,
  SortSetting,
} from "../sortedtable/sortedtable";
import {
  ActiveTransactionsTable,
  getColumnOptions,
} from "./activeTransactionsTable";
import { useTableSortFromURL } from "../sortedtable/useTableSortFromURL";
import { TransactionViewType } from "src/transactionsPage/transactionsPageTypes";

const sortableTableCx = classNames.bind(sortableTableStyles);

const SEARCH_QUERY_PARAM = "q";

type ActiveTransactionsSectionProps = {
  filters: ActiveStatementFilters;
  pagination: ISortedTablePagination;
  search: string;
  transactions: ActiveTransaction[];
  selectedColumns?: string[];
  onClearFilters: () => void;
  onChangeSortSetting: (ss: SortSetting) => void;
  onColumnsSelect: (columns: string[]) => void;
};

export const ActiveTransactionsSection: React.FC<ActiveTransactionsSectionProps> = ({
  filters,
  pagination,
  search,
  transactions,
  selectedColumns,
  onChangeSortSetting,
  onClearFilters,
  onColumnsSelect,
}) => {
  const queryParams = useQueryParmeters();

  const tableColumns: SelectOption[] = getColumnOptions(selectedColumns);
  const data = transactions || [];
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
      <ActiveTransactionsTable
        data={data}
        selectedColumns={selectedColumns}
        sortSetting={sortSetting}
        onChangeSortSetting={onChangeSortSetting}
        renderNoResult={
          <EmptyTransactionsPlaceholder
            isEmptySearchResults={search?.length > 0 && data?.length > 0}
            transactionView={TransactionViewType.ACTIVE}
          />
        }
        pagination={pagination}
      />
    </section>
  );
};
