// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect, useState } from "react";
import classNames from "classnames/bind";
import { useHistory } from "react-router-dom";
import {
  ISortedTablePagination,
  SortSetting,
} from "src/sortedtable/sortedtable";
import { Loading } from "src/loading/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig/pageConfig";
import { Search } from "src/search/search";
import {
  ActiveTransaction,
  ActiveStatementFilters,
  ActiveTransactionFilters,
} from "src/activeExecutions";
import LoadingError from "src/sqlActivity/errorComponent";
import {
  calculateActiveFilters,
  Filter,
  getFullFiltersAsStringRecord,
  inactiveFiltersState,
} from "../queryFilter";
import {
  getAppsFromActiveExecutions,
  getExecututionStatuses,
} from "../activeExecutions/activeStatementUtils";
import { ActiveTransactionsSection } from "src/activeExecutions/activeTransactionsSection";
import { Pagination } from "src/pagination";

import styles from "../statementsPage/statementsPage.module.scss";
import { queryByName, syncHistory } from "src/util/query";
import { getTableSortFromURL } from "src/sortedtable/getTableSortFromURL";
import { getActiveTransactionFiltersFromURL } from "src/queryFilter/utils";
import { filterActiveTransactions } from "../activeExecutions/activeStatementUtils";
import { InlineAlert } from "@cockroachlabs/ui-components";
const cx = classNames.bind(styles);

export type ActiveTransactionsViewDispatchProps = {
  onColumnsSelect: (columns: string[]) => void;
  onFiltersChange: (filters: ActiveTransactionFilters) => void;
  onSortChange: (ss: SortSetting) => void;
  refreshLiveWorkload: () => void;
};

export type ActiveTransactionsViewStateProps = {
  selectedColumns: string[];
  transactions: ActiveTransaction[];
  sessionsError: Error | null;
  filters: ActiveTransactionFilters;
  sortSetting: SortSetting;
  internalAppNamePrefix: string;
  isTenant?: boolean;
  maxSizeApiReached?: boolean;
};

export type ActiveTransactionsViewProps = ActiveTransactionsViewStateProps &
  ActiveTransactionsViewDispatchProps;

const RECENT_TXN_SEARCH_PARAM = "q";
const PAGE_SIZE = 20;

export const ActiveTransactionsView: React.FC<ActiveTransactionsViewProps> = ({
  onColumnsSelect,
  refreshLiveWorkload,
  onFiltersChange,
  onSortChange,
  isTenant,
  selectedColumns,
  sortSetting,
  transactions,
  sessionsError,
  filters,
  internalAppNamePrefix,
  maxSizeApiReached,
}: ActiveTransactionsViewProps) => {
  const [pagination, setPagination] = useState<ISortedTablePagination>({
    current: 1,
    pageSize: PAGE_SIZE,
  });

  const history = useHistory();
  const [search, setSearch] = useState<string>(
    queryByName(history.location, RECENT_TXN_SEARCH_PARAM),
  );

  useEffect(() => {
    // Refresh  every 10 seconds.
    refreshLiveWorkload();

    const interval = setInterval(refreshLiveWorkload, 10 * 1000);
    return () => {
      clearInterval(interval);
    };
  }, [refreshLiveWorkload]);

  useEffect(() => {
    // We use this effect to sync settings defined on the URL (sort, filters),
    // with the redux store. The only time we do this is when the user navigates
    // to the page directly via the URL and specifies settings in the query string.
    // Note that the desired behaviour is currently that the user is unable to
    // clear filters via the URL, and must do so with page controls.
    const sortSettingURL = getTableSortFromURL(history.location);
    const filtersFromURL = getActiveTransactionFiltersFromURL(history.location);

    if (sortSettingURL) {
      onSortChange(sortSettingURL);
    }
    if (filtersFromURL) {
      onFiltersChange(filtersFromURL);
    }
  }, [history, onSortChange, onFiltersChange]);

  useEffect(() => {
    // This effect runs when the filters or sort settings received from
    // redux changes and syncs the URL params with redux.
    syncHistory(
      {
        ascending: sortSetting.ascending.toString(),
        columnTitle: sortSetting.columnTitle,
        ...getFullFiltersAsStringRecord(filters),
        [RECENT_TXN_SEARCH_PARAM]: search,
      },
      history,
    );
  }, [
    history,
    filters,
    sortSetting.ascending,
    sortSetting.columnTitle,
    search,
  ]);

  const resetPagination = () => {
    setPagination({
      current: 1,
      pageSize: PAGE_SIZE,
    });
  };

  const onChangeSortSetting = (ss: SortSetting): void => {
    onSortChange(ss);
    resetPagination();
  };

  const onSubmitSearch = (newSearch: string) => {
    if (newSearch === search) return;
    setSearch(newSearch);
    resetPagination();
  };

  const onSubmitFilters = (selectedFilters: ActiveStatementFilters) => {
    onFiltersChange(selectedFilters);
    resetPagination();
  };

  const clearSearch = () => onSubmitSearch("");
  const clearFilters = () =>
    onSubmitFilters({
      app: inactiveFiltersState.app,
      executionStatus: inactiveFiltersState.executionStatus,
    });

  const apps = getAppsFromActiveExecutions(transactions, internalAppNamePrefix);
  const countActiveFilters = calculateActiveFilters(filters);
  const executionStatuses = getExecututionStatuses();

  const filteredTransactions = filterActiveTransactions(
    transactions,
    filters,
    internalAppNamePrefix,
    search,
  );

  const onChangePage = (page: number) => {
    setPagination({
      ...pagination,
      current: page,
    });
  };

  return (
    <div className={cx("root")}>
      <PageConfig>
        <PageConfigItem>
          <Search
            placeholder="Search Transactions"
            onSubmit={onSubmitSearch}
            onClear={clearSearch}
            defaultValue={search}
          />
        </PageConfigItem>
        <PageConfigItem>
          <Filter
            activeFilters={countActiveFilters}
            onSubmitFilters={onSubmitFilters}
            executionStatuses={executionStatuses}
            showExecutionStatus={true}
            appNames={apps}
            filters={filters}
          />
        </PageConfigItem>
      </PageConfig>
      <div className={cx("table-area")}>
        <Loading
          loading={transactions == null}
          page="active transactions"
          error={sessionsError}
          renderError={() =>
            LoadingError({
              statsType: "transactions",
            })
          }
        >
          <ActiveTransactionsSection
            filters={filters}
            pagination={pagination}
            search={search}
            transactions={filteredTransactions}
            selectedColumns={selectedColumns}
            sortSetting={sortSetting}
            onClearFilters={clearFilters}
            onChangeSortSetting={onChangeSortSetting}
            onColumnsSelect={onColumnsSelect}
            isTenant={isTenant}
          />
          <Pagination
            pageSize={pagination.pageSize}
            current={pagination.current}
            total={filteredTransactions?.length}
            onChange={onChangePage}
          />
          {maxSizeApiReached && (
            <InlineAlert
              intent="info"
              title={
                <>
                  Not all contention events are displayed because the maximum
                  number of contention events was reached in the console.
                </>
              }
            />
          )}
        </Loading>
      </div>
    </div>
  );
};
