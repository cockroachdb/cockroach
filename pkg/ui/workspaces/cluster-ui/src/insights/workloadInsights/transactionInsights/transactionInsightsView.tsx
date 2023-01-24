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
  calculateActiveFilters,
  Filter,
  getFullFiltersAsStringRecord,
} from "src/queryFilter/filter";
import { getWorkloadInsightEventFiltersFromURL } from "src/queryFilter/utils";
import { Pagination } from "src/pagination";
import { queryByName, syncHistory } from "src/util/query";
import { getTableSortFromURL } from "src/sortedtable/getTableSortFromURL";
import { TableStatistics } from "src/tableStatistics";

import { TransactionInsightEventsResponse } from "src/api/insightsApi";
import {
  filterTransactionInsights,
  getAppsFromTransactionInsights,
  getInsightsFromState,
  WorkloadInsightEventFilters,
} from "src/insights";
import { EmptyInsightsTablePlaceholder } from "../util";
import { TransactionInsightsTable } from "./transactionInsightsTable";
import { InsightsError } from "../../insightsErrorComponent";

import styles from "src/statementsPage/statementsPage.module.scss";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { TimeScale } from "../../../timeScaleDropdown";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

export type TransactionInsightsViewStateProps = {
  transactions: TransactionInsightEventsResponse;
  transactionsError: Error | null;
  filters: WorkloadInsightEventFilters;
  sortSetting: SortSetting;
  dropDownSelect?: React.ReactElement;
};

export type TransactionInsightsViewDispatchProps = {
  onFiltersChange: (filters: WorkloadInsightEventFilters) => void;
  onSortChange: (ss: SortSetting) => void;
  refreshTransactionInsights: () => void;
  setTimeScale: (ts: TimeScale) => void;
};

export type TransactionInsightsViewProps = TransactionInsightsViewStateProps &
  TransactionInsightsViewDispatchProps;

const INSIGHT_TXN_SEARCH_PARAM = "q";
const INTERNAL_APP_NAME_PREFIX = "$ internal";

export const TransactionInsightsView: React.FC<TransactionInsightsViewProps> = (
  props: TransactionInsightsViewProps,
) => {
  const {
    sortSetting,
    transactions,
    transactionsError,
    filters,
    refreshTransactionInsights,
    onFiltersChange,
    onSortChange,
    setTimeScale,
    dropDownSelect,
  } = props;

  const [pagination, setPagination] = useState<ISortedTablePagination>({
    current: 1,
    pageSize: 10,
  });
  const history = useHistory();
  const [search, setSearch] = useState<string>(
    queryByName(history.location, INSIGHT_TXN_SEARCH_PARAM),
  );

  useEffect(() => {
    // Refresh every 20 seconds.
    refreshTransactionInsights();
    const interval = setInterval(refreshTransactionInsights, 20 * 1000);
    return () => {
      clearInterval(interval);
    };
  }, [refreshTransactionInsights]);

  useEffect(() => {
    // We use this effect to sync settings defined on the URL (sort, filters),
    // with the redux store. The only time we do this is when the user navigates
    // to the page directly via the URL and specifies settings in the query string.
    // Note that the desired behaviour is currently that the user is unable to
    // clear filters via the URL, and must do so with page controls.
    const sortSettingURL = getTableSortFromURL(history.location);
    const filtersFromURL = getWorkloadInsightEventFiltersFromURL(
      history.location,
    );

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
        [INSIGHT_TXN_SEARCH_PARAM]: search,
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

  const onChangePage = (current: number): void => {
    setPagination({
      current: current,
      pageSize: 10,
    });
  };

  const resetPagination = () => {
    setPagination({
      current: 1,
      pageSize: 10,
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

  const clearSearch = () => onSubmitSearch("");

  const onSubmitFilters = (selectedFilters: WorkloadInsightEventFilters) => {
    onFiltersChange(selectedFilters);
    resetPagination();
  };

  const clearFilters = () =>
    onSubmitFilters({
      app: "",
    });

  const transactionInsights = getInsightsFromState(transactions);
  const apps = getAppsFromTransactionInsights(
    transactionInsights,
    INTERNAL_APP_NAME_PREFIX,
  );
  const countActiveFilters = calculateActiveFilters(filters);
  const filteredTransactions = filterTransactionInsights(
    transactionInsights,
    filters,
    INTERNAL_APP_NAME_PREFIX,
    search,
  );

  return (
    <div className={cx("root")}>
      <PageConfig>
        <PageConfigItem>{dropDownSelect}</PageConfigItem>
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
            appNames={apps}
            filters={filters}
          />
        </PageConfigItem>
      </PageConfig>
      <div className={cx("table-area")}>
        <Loading
          loading={transactions === null}
          page="transaction insights"
          error={transactionsError}
          renderError={() => InsightsError(transactionsError?.message)}
        >
          <div>
            <section className={sortableTableCx("cl-table-container")}>
              <div>
                <TableStatistics
                  pagination={pagination}
                  search={search}
                  totalCount={filteredTransactions?.length}
                  arrayItemName="transaction insights"
                  activeFilters={countActiveFilters}
                  onClearFilters={clearFilters}
                />
              </div>
              <TransactionInsightsTable
                data={filteredTransactions}
                sortSetting={sortSetting}
                onChangeSortSetting={onChangeSortSetting}
                setTimeScale={setTimeScale}
                renderNoResult={
                  <EmptyInsightsTablePlaceholder
                    isEmptySearchResults={
                      search?.length > 0 && filteredTransactions?.length === 0
                    }
                  />
                }
                pagination={pagination}
              />
            </section>
            <Pagination
              pageSize={pagination.pageSize}
              current={pagination.current}
              total={filteredTransactions?.length}
              onChange={onChangePage}
            />
          </div>
        </Loading>
      </div>
    </div>
  );
};
