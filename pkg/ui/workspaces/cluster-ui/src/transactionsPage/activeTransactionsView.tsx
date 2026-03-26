// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React, { useContext, useEffect, useState } from "react";
import { useHistory } from "react-router-dom";

import {
  ActiveTransactionFilters,
  ExecutionStatus,
} from "src/activeExecutions";
import { ActiveTransactionsSection } from "src/activeExecutions/activeTransactionsSection";
import RefreshControl from "src/activeExecutions/refreshControl/refreshControl";
import { useLiveWorkload } from "src/api/liveWorkloadApi";
import { Loading } from "src/loading/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig/pageConfig";
import { Pagination } from "src/pagination";
import { Filter } from "src/queryFilter";
import { getActiveTransactionFiltersFromURL } from "src/queryFilter/utils";
import { Search } from "src/search/search";
import { SortSetting } from "src/sortedtable/sortedtable";
import LoadingError from "src/sqlActivity/errorComponent";
import { queryByName, syncHistory } from "src/util/query";

import {
  filterActiveTransactions,
  getAppsFromActiveExecutions,
  useDisplayRefreshAlert,
} from "../activeExecutions/activeStatementUtils";
import { ClusterDetailsContext } from "../contexts";
import {
  calculateActiveFilters,
  defaultFilters,
  getFullFiltersAsStringRecord,
} from "../queryFilter";
import { getTableSortFromURL } from "../sortedtable/getTableSortFromURL";
import styles from "../statementsPage/statementsPage.module.scss";
import { usePagination } from "../util";

const cx = classNames.bind(styles);

const RECENT_TXN_SEARCH_PARAM = "q";
const PAGE_SIZE = 20;

const DEFAULT_SORT_SETTING: SortSetting = {
  ascending: false,
  columnTitle: "startTime",
};

const DEFAULT_FILTERS: ActiveTransactionFilters = {
  app: defaultFilters.app,
  executionStatus: defaultFilters.executionStatus,
};

export const ActiveTransactionsView: React.FC = () => {
  const { isTenant } = useContext(ClusterDetailsContext);
  const history = useHistory();

  // Initialize sort and filters from URL params, falling back to defaults.
  const [sortSetting, setSortSetting] = useState<SortSetting>(
    () => getTableSortFromURL(history.location) ?? DEFAULT_SORT_SETTING,
  );
  const [filters, setFilters] = useState<ActiveTransactionFilters>(
    () =>
      ({
        ...DEFAULT_FILTERS,
        ...getActiveTransactionFiltersFromURL(history.location),
      }) as ActiveTransactionFilters,
  );
  const [search, setSearch] = useState<string>(
    queryByName(history.location, RECENT_TXN_SEARCH_PARAM),
  );
  const [selectedColumns, setSelectedColumns] = useState<string[] | null>(null);
  const [isAutoRefreshEnabled, setIsAutoRefreshEnabled] = useState(true);

  const { data, isLoading, error, lastUpdated, refresh } = useLiveWorkload({
    refreshInterval: isAutoRefreshEnabled ? 10_000 : 0,
  });
  const transactions = data.transactions;
  const internalAppNamePrefix = data.internalAppNamePrefix;
  const maxSizeApiReached = data.maxSizeApiReached;

  const [pagination, updatePagination, resetPagination] = usePagination(
    1,
    PAGE_SIZE,
  );

  const { displayRefreshAlert, minutesSinceLastRefresh } =
    useDisplayRefreshAlert(isAutoRefreshEnabled, lastUpdated);

  // Sync sort, filters, and search to URL whenever they change.
  useEffect(() => {
    syncHistory(
      {
        ascending: sortSetting.ascending.toString(),
        columnTitle: sortSetting.columnTitle,
        [RECENT_TXN_SEARCH_PARAM]: search,
        ...getFullFiltersAsStringRecord(filters),
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

  const onChangeSortSetting = (ss: SortSetting): void => {
    setSortSetting(ss);
    resetPagination();
  };

  const onSubmitSearch = (newSearch: string): void => {
    if (newSearch === search) return;
    setSearch(newSearch);
    resetPagination();
  };

  const onSubmitFilters = (selectedFilters: ActiveTransactionFilters) => {
    setFilters(selectedFilters);
    resetPagination();
  };

  const onSubmitToggleAutoRefresh = () => {
    // Refresh immediately when toggling auto-refresh on.
    if (!isAutoRefreshEnabled) {
      refresh();
    }
    setIsAutoRefreshEnabled(!isAutoRefreshEnabled);
  };

  const clearSearch = () => onSubmitSearch("");
  const clearFilters = () => onSubmitFilters(DEFAULT_FILTERS);

  const apps = getAppsFromActiveExecutions(transactions, internalAppNamePrefix);
  const countActiveFilters = calculateActiveFilters(filters);
  const executionStatuses = Object.values(ExecutionStatus);

  const filteredTransactions = filterActiveTransactions(
    transactions,
    filters,
    internalAppNamePrefix,
    search,
  );

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
        <PageConfigItem>
          <RefreshControl
            isAutoRefreshEnabled={isAutoRefreshEnabled}
            onToggleAutoRefresh={onSubmitToggleAutoRefresh}
            onManualRefresh={refresh}
            lastRefreshTimestamp={lastUpdated}
            execType={"transaction"}
          />
        </PageConfigItem>
      </PageConfig>
      {displayRefreshAlert && (
        <div className={cx("refresh-alert")}>
          <InlineAlert
            intent="warning"
            title={
              <>
                Your active transactions data is {minutesSinceLastRefresh}{" "}
                minutes old. Consider refreshing for the latest information.
              </>
            }
          />
        </div>
      )}
      <div className={cx("table-area")}>
        <Loading
          loading={isLoading}
          page="active transactions"
          error={error}
          renderError={() =>
            LoadingError({
              statsType: "transactions",
              error,
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
            onColumnsSelect={setSelectedColumns}
            isTenant={isTenant}
          />
          <Pagination
            pageSize={pagination.pageSize}
            current={pagination.current}
            total={filteredTransactions?.length}
            onChange={updatePagination}
            onShowSizeChange={updatePagination}
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
