// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React, { useContext, useEffect, useState } from "react";
import { useHistory } from "react-router-dom";

import { ActiveStatementFilters, ExecutionStatus } from "src/activeExecutions";
import RefreshControl from "src/activeExecutions/refreshControl/refreshControl";
import { useLiveWorkload } from "src/api/liveWorkloadApi";
import { Loading } from "src/loading/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig/pageConfig";
import { Pagination } from "src/pagination";
import { Filter } from "src/queryFilter";
import { getActiveStatementFiltersFromURL } from "src/queryFilter/utils";
import { Search } from "src/search/search";
import { SortSetting } from "src/sortedtable/sortedtable";
import LoadingError from "src/sqlActivity/errorComponent";
import { queryByName, syncHistory } from "src/util/query";

import { ActiveStatementsSection } from "../activeExecutions/activeStatementsSection";
import {
  ACTIVE_STATEMENT_SEARCH_PARAM,
  getAppsFromActiveExecutions,
  filterActiveStatements,
  useDisplayRefreshAlert,
} from "../activeExecutions/activeStatementUtils";
import { ClusterDetailsContext } from "../contexts";
import {
  calculateActiveFilters,
  defaultFilters,
  getFullFiltersAsStringRecord,
} from "../queryFilter";
import { getTableSortFromURL } from "../sortedtable/getTableSortFromURL";
import { usePagination } from "../util";

import styles from "./statementsPage.module.scss";

const cx = classNames.bind(styles);
const PAGE_SIZE = 20;

const DEFAULT_SORT_SETTING: SortSetting = {
  ascending: false,
  columnTitle: "startTime",
};

const DEFAULT_FILTERS: ActiveStatementFilters = {
  app: defaultFilters.app,
  executionStatus: defaultFilters.executionStatus,
};

export const ActiveStatementsView: React.FC = () => {
  const { isTenant } = useContext(ClusterDetailsContext);
  const history = useHistory();

  // Initialize sort and filters from URL params, falling back to defaults.
  const [sortSetting, setSortSetting] = useState<SortSetting>(
    () => getTableSortFromURL(history.location) ?? DEFAULT_SORT_SETTING,
  );
  const [filters, setFilters] = useState<ActiveStatementFilters>(
    () =>
      ({
        ...DEFAULT_FILTERS,
        ...getActiveStatementFiltersFromURL(history.location),
      }) as ActiveStatementFilters,
  );
  const [search, setSearch] = useState<string>(
    queryByName(history.location, ACTIVE_STATEMENT_SEARCH_PARAM),
  );
  const [selectedColumns, setSelectedColumns] = useState<string[] | null>(null);
  const [isAutoRefreshEnabled, setIsAutoRefreshEnabled] = useState(true);

  const { data, isLoading, error, lastUpdated, refresh } = useLiveWorkload({
    refreshInterval: isAutoRefreshEnabled ? 10_000 : 0,
  });
  const statements = data.statements;
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
        [ACTIVE_STATEMENT_SEARCH_PARAM]: search,
        ...getFullFiltersAsStringRecord(filters),
      },
      history,
      true,
    );
  }, [
    history,
    filters,
    sortSetting.ascending,
    sortSetting.columnTitle,
    search,
  ]);

  const onSortClick = (ss: SortSetting): void => {
    setSortSetting(ss);
    resetPagination();
  };

  const onSubmitSearch = (newSearch: string): void => {
    if (newSearch === search) return;
    setSearch(newSearch);
    resetPagination();
  };

  const onSubmitFilters = (selectedFilters: ActiveStatementFilters) => {
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

  const apps = getAppsFromActiveExecutions(statements, internalAppNamePrefix);
  const countActiveFilters = calculateActiveFilters(filters);
  // The "Idle" execution status does not apply to statements.
  const executionStatuses = Object.values(ExecutionStatus).filter(
    status => status !== ExecutionStatus.Idle,
  );
  const filteredStatements = filterActiveStatements(
    statements,
    filters,
    internalAppNamePrefix,
    search,
  );

  return (
    <div className={cx("root")}>
      <PageConfig>
        <PageConfigItem>
          <Search
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
            execType={"statement"}
          />
        </PageConfigItem>
      </PageConfig>
      {displayRefreshAlert && (
        <div className={cx("refresh-alert")}>
          <InlineAlert
            intent="warning"
            title={
              <>
                Your active statements data is {minutesSinceLastRefresh} minutes
                old. Consider refreshing for the latest information.
              </>
            }
          />
        </div>
      )}
      <div className={cx("table-area")}>
        <Loading
          loading={isLoading}
          page="active statements"
          error={error}
          renderError={() =>
            LoadingError({
              statsType: "statements",
              error,
            })
          }
        >
          <ActiveStatementsSection
            filters={filters}
            pagination={pagination}
            search={search}
            statements={filteredStatements}
            selectedColumns={selectedColumns}
            sortSetting={sortSetting}
            onClearFilters={clearFilters}
            onChangeSortSetting={onSortClick}
            onColumnsSelect={setSelectedColumns}
            isTenant={isTenant}
          />
          <Pagination
            pageSize={pagination.pageSize}
            current={pagination.current}
            total={filteredStatements?.length}
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
