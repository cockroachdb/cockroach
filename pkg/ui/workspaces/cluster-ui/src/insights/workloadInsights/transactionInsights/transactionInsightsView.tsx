// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React, { useCallback, useEffect, useState } from "react";
import { useHistory } from "react-router-dom";

import { Anchor } from "src/anchor";
import { TxnInsightsRequest } from "src/api";
import {
  filterTransactionInsights,
  getAppsFromTransactionInsights,
  WorkloadInsightEventFilters,
  TxnInsightEvent,
} from "src/insights";
import { Loading } from "src/loading/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig/pageConfig";
import { Pagination } from "src/pagination";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  getFullFiltersAsStringRecord,
  SelectedFilters,
} from "src/queryFilter/filter";
import { getWorkloadInsightEventFiltersFromURL } from "src/queryFilter/utils";
import { Search } from "src/search/search";
import { getTableSortFromURL } from "src/sortedtable/getTableSortFromURL";
import {
  ISortedTablePagination,
  SortSetting,
} from "src/sortedtable/sortedtable";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import styles from "src/statementsPage/statementsPage.module.scss";
import { TableStatistics } from "src/tableStatistics";
import { insights } from "src/util";
import { useScheduleFunction } from "src/util/hooks";
import { queryByName, syncHistory } from "src/util/query";

import { commonStyles } from "../../../common";
import {
  TimeScale,
  defaultTimeScaleOptions,
  TimeScaleDropdown,
  timeScaleRangeToObj,
} from "../../../timeScaleDropdown";
import { InsightsError } from "../../insightsErrorComponent";
import { EmptyInsightsTablePlaceholder } from "../util";

import { TransactionInsightsTable } from "./transactionInsightsTable";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

export type TransactionInsightsViewStateProps = {
  isDataValid: boolean;
  lastUpdated: moment.Moment;
  transactions: TxnInsightEvent[];
  transactionsError: Error | null;
  insightTypes: string[];
  filters: WorkloadInsightEventFilters;
  sortSetting: SortSetting;
  isLoading?: boolean;
  dropDownSelect?: React.ReactElement;
  timeScale?: TimeScale;
  maxSizeApiReached?: boolean;
};

export type TransactionInsightsViewDispatchProps = {
  onFiltersChange: (filters: WorkloadInsightEventFilters) => void;
  onSortChange: (ss: SortSetting) => void;
  refreshTransactionInsights: (req: TxnInsightsRequest) => void;
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
    isDataValid,
    lastUpdated,
    sortSetting,
    transactions,
    transactionsError,
    insightTypes,
    filters,
    timeScale,
    isLoading,
    refreshTransactionInsights,
    onFiltersChange,
    onSortChange,
    setTimeScale,
    dropDownSelect,
    maxSizeApiReached,
  } = props;

  const [pagination, setPagination] = useState<ISortedTablePagination>({
    current: 1,
    pageSize: 10,
  });
  const history = useHistory();
  const [search, setSearch] = useState<string>(
    queryByName(history.location, INSIGHT_TXN_SEARCH_PARAM),
  );

  const refresh = useCallback(() => {
    const req = timeScaleRangeToObj(timeScale);
    refreshTransactionInsights(req);
  }, [refreshTransactionInsights, timeScale]);

  const shouldPoll = timeScale.key !== "Custom";
  const [refetch, clearPolling] = useScheduleFunction(
    refresh,
    shouldPoll, // Don't reschedule refresh if we have a custom time interval.
    10 * 1000, // 10s polling interval
    lastUpdated,
  );

  useEffect(() => {
    if (!isDataValid) refetch();
  }, [isDataValid, refetch]);

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
      app: defaultFilters.app,
      workloadInsightType: defaultFilters.workloadInsightType,
    });

  const transactionInsights = transactions;
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

  const onTimeScaleChange = useCallback(
    (ts: TimeScale) => {
      clearPolling();
      setTimeScale(ts);
    },
    [clearPolling, setTimeScale],
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
            workloadInsightTypes={insightTypes.sort()}
            showWorkloadInsightTypes={true}
          />
        </PageConfigItem>
        <PageConfigItem className={commonStyles("separator")}>
          <TimeScaleDropdown
            options={defaultTimeScaleOptions}
            currentScale={timeScale}
            setTimeScale={onTimeScaleChange}
          />
        </PageConfigItem>
      </PageConfig>
      <SelectedFilters
        filters={filters}
        onRemoveFilter={onSubmitFilters}
        onClearFilters={clearFilters}
        className={cx("margin-adjusted")}
      />
      <div className={cx("table-area")}>
        <Loading
          loading={isLoading}
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
                />
              </div>
              <TransactionInsightsTable
                data={filteredTransactions}
                sortSetting={sortSetting}
                onChangeSortSetting={onChangeSortSetting}
                setTimeScale={onTimeScaleChange}
                renderNoResult={
                  <EmptyInsightsTablePlaceholder
                    isEmptySearchResults={
                      (search?.length > 0 || countActiveFilters > 0) &&
                      filteredTransactions?.length === 0
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
            {maxSizeApiReached && (
              <InlineAlert
                intent="info"
                title={
                  <>
                    Not all insights are displayed because the maximum number of
                    insights was reached in the console.&nbsp;
                    <Anchor href={insights} target="_blank">
                      Learn more
                    </Anchor>
                  </>
                }
              />
            )}
          </div>
        </Loading>
      </div>
    </div>
  );
};
