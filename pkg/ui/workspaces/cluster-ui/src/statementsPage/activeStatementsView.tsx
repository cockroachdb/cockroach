// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import moment, { Moment } from "moment-timezone";
import React, { useEffect, useState } from "react";
import { useHistory } from "react-router-dom";

import {
  ActiveStatement,
  ActiveStatementFilters,
  ExecutionStatus,
} from "src/activeExecutions";
import RefreshControl from "src/activeExecutions/refreshControl/refreshControl";
import { Loading } from "src/loading/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig/pageConfig";
import { Pagination } from "src/pagination";
import { Filter } from "src/queryFilter";
import { getActiveStatementFiltersFromURL } from "src/queryFilter/utils";
import { Search } from "src/search/search";
import {
  ISortedTablePagination,
  SortSetting,
} from "src/sortedtable/sortedtable";
import LoadingError from "src/sqlActivity/errorComponent";
import { queryByName, syncHistory } from "src/util/query";

import { ActiveStatementsSection } from "../activeExecutions/activeStatementsSection";
import {
  ACTIVE_STATEMENT_SEARCH_PARAM,
  getAppsFromActiveExecutions,
  filterActiveStatements,
} from "../activeExecutions/activeStatementUtils";
import {
  calculateActiveFilters,
  defaultFilters,
  getFullFiltersAsStringRecord,
} from "../queryFilter";
import { getTableSortFromURL } from "../sortedtable/getTableSortFromURL";

import styles from "./statementsPage.module.scss";

const cx = classNames.bind(styles);
const PAGE_SIZE = 20;

export type ActiveStatementsViewDispatchProps = {
  onColumnsSelect: (columns: string[]) => void;
  onFiltersChange: (filters: ActiveStatementFilters) => void;
  onSortChange: (ss: SortSetting) => void;
  refreshLiveWorkload: () => void;
  onAutoRefreshToggle: (isEnabled: boolean) => void;
  onManualRefresh: () => void;
};

export type ActiveStatementsViewStateProps = {
  selectedColumns: string[];
  statements: ActiveStatement[];
  sortSetting: SortSetting;
  sessionsError: Error | null;
  filters: ActiveStatementFilters;
  internalAppNamePrefix: string;
  isTenant?: boolean;
  maxSizeApiReached?: boolean;
  isAutoRefreshEnabled?: boolean;
  lastUpdated: Moment | null;
};

export type ActiveStatementsViewProps = ActiveStatementsViewStateProps &
  ActiveStatementsViewDispatchProps;

export const ActiveStatementsView: React.FC<ActiveStatementsViewProps> = ({
  onColumnsSelect,
  refreshLiveWorkload,
  onFiltersChange,
  onSortChange,
  selectedColumns,
  sortSetting,
  statements,
  sessionsError,
  filters,
  internalAppNamePrefix,
  isTenant,
  maxSizeApiReached,
  isAutoRefreshEnabled,
  onAutoRefreshToggle,
  lastUpdated,
  onManualRefresh,
}: ActiveStatementsViewProps) => {
  const [pagination, setPagination] = useState<ISortedTablePagination>({
    current: 1,
    pageSize: PAGE_SIZE,
  });
  const history = useHistory();
  const [search, setSearch] = useState<string>(
    queryByName(history.location, ACTIVE_STATEMENT_SEARCH_PARAM),
  );
  // Local state to store the difference between the current time and the last
  // time the data was updated, in minutes.
  const [minutesSinceLastRefresh, setMinutesSinceLastRefresh] = useState(0);
  // Local state to store whether or not to display the refresh alert.
  const [displayRefreshAlert, setDisplayRefreshAlert] = useState(false);

  useEffect(() => {
    // useEffect hook which triggers an immediate data refresh if auto-refresh
    // is enabled. It fetches the latest workload details by dispatching a
    // refresh action when the component mounts, ensuring that users see fresh
    // data as soon as they land on the page if auto-refresh is on.
    if (isAutoRefreshEnabled) {
      refreshLiveWorkload();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    // Refresh the workload every 10 seconds if auto refresh is on.
    if (isAutoRefreshEnabled) {
      const interval = setInterval(refreshLiveWorkload, 10 * 1000);
      return () => {
        clearInterval(interval);
      };
    }
  }, [isAutoRefreshEnabled, refreshLiveWorkload]);

  useEffect(() => {
    // This useEffect hook checks the difference between the current time and
    // the last time the data was updated. It triggers a state change to display
    // an alert if the difference is greater than 10 minutes and auto-refresh
    // is disabled. The check is performed immediately when the component mounts
    // and then every 10 seconds thereafter.
    const checkTimeDifference = () => {
      if (!isAutoRefreshEnabled && lastUpdated) {
        // Calculate the difference between the last updated time and the current time in minutes
        const diffMinutes = moment().diff(lastUpdated, "minutes");
        if (diffMinutes >= 10) {
          setDisplayRefreshAlert(true);
          setMinutesSinceLastRefresh(diffMinutes);
        } else {
          setDisplayRefreshAlert(false);
        }
      }
    };

    checkTimeDifference();
    const intervalId = setInterval(checkTimeDifference, 10 * 1000);

    return () => {
      clearInterval(intervalId);
    };
  }, [lastUpdated, isAutoRefreshEnabled, setDisplayRefreshAlert]);

  useEffect(() => {
    // We use this effect to sync settings defined on the URL (sort, filters),
    // with the redux store. The only time we do this is when the user navigates
    // to the page directly via the URL and specifies settings in the query string.
    // Note that the desired behaviour is currently that the user is unable to
    // clear filters via the URL, and must do so with page controls.
    const sortSettingURL = getTableSortFromURL(history.location);
    const filtersFromURL = getActiveStatementFiltersFromURL(history.location);

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

  const resetPagination = () => {
    setPagination({
      pageSize: PAGE_SIZE,
      current: 1,
    });
  };

  const onSortClick = (ss: SortSetting): void => {
    onSortChange(ss);
    resetPagination();
  };

  const onSubmitSearch = (newSearch: string): void => {
    if (newSearch === search) return;
    setSearch(newSearch);
    resetPagination();
  };

  const onSubmitFilters = (selectedFilters: ActiveStatementFilters) => {
    onFiltersChange(selectedFilters);
    resetPagination();
  };

  const onSubmitToggleAutoRefresh = () => {
    // Refresh immediately when toggling auto-refresh on.
    if (!isAutoRefreshEnabled) {
      setDisplayRefreshAlert(false);
      refreshLiveWorkload();
    }
    onAutoRefreshToggle(!isAutoRefreshEnabled);
  };

  const handleRefresh = () => {
    onManualRefresh();
  };

  const clearSearch = () => onSubmitSearch("");
  const clearFilters = () =>
    onSubmitFilters({
      app: defaultFilters.app,
      executionStatus: defaultFilters.executionStatus,
    });

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
            onManualRefresh={handleRefresh}
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
          loading={statements == null}
          page="active statements"
          error={sessionsError}
          renderError={() =>
            LoadingError({
              statsType: "statements",
              error: sessionsError,
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
            onColumnsSelect={onColumnsSelect}
            isTenant={isTenant}
          />
          <Pagination
            pageSize={pagination.pageSize}
            current={pagination.current}
            total={filteredStatements?.length}
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
