// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import flatMap from "lodash/flatMap";
import isString from "lodash/isString";
import moment from "moment-timezone";
import React, { useState, useEffect, useCallback, useRef } from "react";
import { RouteComponentProps } from "react-router-dom";

import {
  SqlStatsSortType,
  createCombinedStmtsRequest,
  StatementsRequest,
  SqlStatsSortOptions,
  SqlStatsResponse,
} from "src/api/statementsApi";
import { SearchCriteria } from "src/searchCriteria/searchCriteria";
import { TimeScaleLabel } from "src/timeScaleDropdown/timeScaleLabel";
import { Timestamp, TimestampToMoment, syncHistory, unique } from "src/util";
import {
  STATS_LONG_LOADING_DURATION,
  getSortLabel,
  getSortColumn,
  getSubsetWarning,
  getReqSortColumn,
} from "src/util/sqlActivityConstants";

import { RequestState } from "../api";
import ColumnsSelector from "../columnsSelector/columnsSelector";
import { isSelectedColumn } from "../columnsSelector/utils";
import { commonStyles } from "../common";
import { Delayed } from "../delayed";
import { Loading } from "../loading";
import { SelectOption } from "../multiSelectCheckbox/multiSelectCheckbox";
import { PageConfig, PageConfigItem } from "../pageConfig";
import { Pagination, ResultsPerPageLabel } from "../pagination";
import {
  Filter,
  Filters,
  defaultFilters,
  handleFiltersFromQueryString,
  updateFiltersQueryParamsOnTab,
  SelectedFilters,
} from "../queryFilter";
import { Search } from "../search";
import {
  handleSortSettingFromQueryString,
  ISortedTablePagination,
  SortSetting,
  updateSortSettingQueryParamsOnTab,
} from "../sortedtable";
import ClearStats from "../sqlActivity/clearStats";
import LoadingError from "../sqlActivity/errorComponent";
import styles from "../statementsPage/statementsPage.module.scss";
import {
  getLabel,
  StatisticTableColumnKeys,
} from "../statsTableUtil/statsTableUtil";
import { UIConfigState } from "../store";
import {
  TimeScale,
  timeScale1hMinOptions,
  getValidOption,
  toRoundedDateRange,
} from "../timeScaleDropdown";
import timeScaleStyles from "../timeScaleDropdown/timeScale.module.scss";
import {
  makeTransactionsColumns,
  TransactionInfo,
  TransactionsTable,
} from "../transactionsTable";

import { EmptyTransactionsPlaceholder } from "./emptyTransactionsPlaceholder";
import { statisticsClasses } from "./transactionsPageClasses";
import { TransactionViewType } from "./transactionsPageTypes";
import {
  generateRegion,
  generateRegionNode,
  getTrxAppFilterOptions,
  searchTransactionsData,
  filterTransactions,
} from "./utils";

const cx = classNames.bind(styles);
const timeScaleStylesCx = classNames.bind(timeScaleStyles);

export interface TransactionsPageStateProps {
  columns: string[];
  txnsResp: RequestState<SqlStatsResponse>;
  timeScale: TimeScale;
  limit: number;
  reqSortSetting: SqlStatsSortType;
  filters: Filters;
  isTenant?: UIConfigState["isTenant"];
  nodeRegions: { [nodeId: string]: string };
  search: string;
  sortSetting: SortSetting;
  hasAdminRole?: UIConfigState["hasAdminRole"];
  requestTime: moment.Moment;
  oldestDataAvailable: Timestamp;
}

export interface TransactionsPageDispatchProps {
  refreshData: (req: StatementsRequest) => void;
  refreshNodes: () => void;
  refreshUserSQLRoles: () => void;
  resetSQLStats: () => void;
  onTimeScaleChange?: (ts: TimeScale) => void;
  onChangeLimit: (limit: number) => void;
  onChangeReqSort: (sort: SqlStatsSortType) => void;
  onColumnsChange?: (selectedColumns: string[]) => void;
  onFilterChange?: (value: Filters) => void;
  onSearchComplete?: (query: string) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onApplySearchCriteria: (ts: TimeScale, limit: number, sort: string) => void;
  onRequestTimeChange: (t: moment.Moment) => void;
}

export type TransactionsPageProps = TransactionsPageStateProps &
  TransactionsPageDispatchProps &
  RouteComponentProps;

interface RequestParams {
  timeScale: TimeScale;
  limit: number;
  reqSortSetting: SqlStatsSortType;
}

function stmtsRequestFromParams(params: RequestParams): StatementsRequest {
  const [start, end] = toRoundedDateRange(params.timeScale);
  return createCombinedStmtsRequest({
    start,
    end,
    limit: params.limit,
    sort: params.reqSortSetting,
  });
}

export function TransactionsPage(
  props: TransactionsPageProps,
): React.ReactElement {
  const {
    columns: userSelectedColumnsToShow,
    txnsResp,
    timeScale: propsTimeScale,
    limit: propsLimit,
    reqSortSetting: propsReqSortSetting,
    filters: propsFilters,
    isTenant,
    nodeRegions,
    search,
    sortSetting,
    hasAdminRole,
    requestTime,
    oldestDataAvailable,
    refreshData,
    refreshNodes,
    refreshUserSQLRoles,
    resetSQLStats,
    onTimeScaleChange,
    onChangeLimit: propsOnChangeLimit,
    onChangeReqSort: propsOnChangeReqSort,
    onColumnsChange,
    onFilterChange,
    onSearchComplete,
    onSortingChange,
    onApplySearchCriteria,
    onRequestTimeChange,
    history,
  } = props;

  // Local state for search criteria (can differ from props until applied)
  const [localTimeScale, setLocalTimeScale] =
    useState<TimeScale>(propsTimeScale);
  const [localLimit, setLocalLimit] = useState<number>(propsLimit);
  const [localReqSortSetting, setLocalReqSortSetting] =
    useState<SqlStatsSortType>(propsReqSortSetting);

  const [filters, setFilters] = useState<Filters | undefined>(() => {
    // Initialize filters from query string
    const latestFilter = handleFiltersFromQueryString(
      history,
      propsFilters,
      onFilterChange,
    );
    return latestFilter;
  });

  const [pagination, setPagination] = useState<ISortedTablePagination>({
    pageSize: 50,
    current: 1,
  });

  // Initialize search from query string
  useEffect(() => {
    const searchParams = new URLSearchParams(history.location.search);
    const searchQuery = searchParams.get("q") || undefined;
    if (onSearchComplete && searchQuery && search !== searchQuery) {
      onSearchComplete(searchQuery);
    }

    // Sort Settings from query string
    handleSortSettingFromQueryString(
      "Transactions",
      history.location.search,
      sortSetting,
      onSortingChange,
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const refreshDataFromState = useCallback((): void => {
    const req = stmtsRequestFromParams({
      timeScale: localTimeScale,
      limit: localLimit,
      reqSortSetting: localReqSortSetting,
    });
    refreshData(req);
  }, [localTimeScale, localLimit, localReqSortSetting, refreshData]);

  const doResetSQLStats = useCallback((): void => {
    resetSQLStats();
  }, [resetSQLStats]);

  // componentDidMount equivalent
  useEffect(() => {
    // In case the user selected an option not available on this page,
    // force a selection of a valid option.
    const ts = getValidOption(propsTimeScale, timeScale1hMinOptions);
    if (ts !== propsTimeScale) {
      setLocalTimeScale(ts);
      if (onTimeScaleChange) {
        onTimeScaleChange(ts);
      }
      onRequestTimeChange(moment());
    } else if (!txnsResp.valid || !txnsResp.data || !txnsResp.lastUpdated) {
      refreshDataFromState();
    }

    if (!isTenant) {
      refreshNodes();
    }

    refreshUserSQLRoles();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Update query params when relevant state changes
  const updateQueryParams = useCallback((): void => {
    const tab = "Transactions";

    // Search
    const searchParams = new URLSearchParams(history.location.search);
    const currentTab = searchParams.get("tab") || "";
    const searchQueryString = searchParams.get("q") || "";
    if (currentTab === tab && search && search !== searchQueryString) {
      syncHistory({ q: search }, history);
    }

    // Filters
    updateFiltersQueryParamsOnTab(tab, filters, history);

    // Sort Setting
    updateSortSettingQueryParamsOnTab(
      tab,
      sortSetting,
      { ascending: false, columnTitle: "executionCount" },
      history,
    );
  }, [history, search, filters, sortSetting]);

  // componentDidUpdate equivalent
  useEffect(() => {
    updateQueryParams();
    if (!isTenant) {
      refreshNodes();
    }
  }, [updateQueryParams, isTenant, refreshNodes]);

  const onChangeSortSetting = useCallback(
    (ss: SortSetting): void => {
      syncHistory(
        {
          ascending: ss.ascending.toString(),
          columnTitle: ss.columnTitle,
        },
        history,
      );
      if (onSortingChange) {
        onSortingChange("Transactions", ss.columnTitle, ss.ascending);
      }
    },
    [history, onSortingChange],
  );

  const isSortSettingSameAsReqSort = useCallback((): boolean => {
    return getSortColumn(propsReqSortSetting) === sortSetting.columnTitle;
  }, [propsReqSortSetting, sortSetting.columnTitle]);

  const onChangePage = useCallback(
    (current: number, pageSize: number): void => {
      setPagination(prev => ({ ...prev, current, pageSize }));
    },
    [],
  );

  const resetPagination = useCallback((): void => {
    setPagination(prev => ({
      current: 1,
      pageSize: prev.pageSize,
    }));
  }, []);

  const onClearSearchField = useCallback((): void => {
    if (onSearchComplete) {
      onSearchComplete("");
    }
    syncHistory({ q: undefined }, history);
  }, [onSearchComplete, history]);

  const onSubmitSearchField = useCallback(
    (searchValue: string): void => {
      if (onSearchComplete) {
        onSearchComplete(searchValue);
      }
      resetPagination();
      syncHistory({ q: searchValue }, history);
    },
    [onSearchComplete, resetPagination, history],
  );

  const onSubmitFilters = useCallback(
    (newFilters: Filters): void => {
      if (onFilterChange) {
        onFilterChange(newFilters);
      }
      setFilters(newFilters);
      resetPagination();
      syncHistory(
        {
          app: newFilters.app,
          timeNumber: newFilters.timeNumber,
          timeUnit: newFilters.timeUnit,
          regions: newFilters.regions,
          nodes: newFilters.nodes,
        },
        history,
      );
    },
    [onFilterChange, resetPagination, history],
  );

  const onClearFilters = useCallback((): void => {
    if (onFilterChange) {
      onFilterChange(defaultFilters);
    }
    setFilters({ ...defaultFilters });
    resetPagination();
    syncHistory(
      {
        app: undefined,
        timeNumber: undefined,
        timeUnit: undefined,
        regions: undefined,
        nodes: undefined,
      },
      history,
    );
  }, [onFilterChange, resetPagination, history]);

  const changeTimeScale = useCallback((ts: TimeScale): void => {
    setLocalTimeScale(ts);
  }, []);

  const onChangeLimit = useCallback((newLimit: number): void => {
    setLocalLimit(newLimit);
  }, []);

  const onChangeReqSort = useCallback((newSort: SqlStatsSortType): void => {
    setLocalReqSortSetting(newSort);
  }, []);

  const updateRequestParams = useCallback((): void => {
    if (propsLimit !== localLimit) {
      propsOnChangeLimit(localLimit);
    }

    if (propsReqSortSetting !== localReqSortSetting) {
      propsOnChangeReqSort(localReqSortSetting);
    }

    if (propsTimeScale !== localTimeScale) {
      if (onTimeScaleChange) {
        onTimeScaleChange(localTimeScale);
      }
    }

    if (onApplySearchCriteria) {
      onApplySearchCriteria(
        localTimeScale,
        localLimit,
        getSortLabel(localReqSortSetting, "Transaction"),
      );
    }
    onRequestTimeChange(moment());

    const req = stmtsRequestFromParams({
      timeScale: localTimeScale,
      limit: localLimit,
      reqSortSetting: localReqSortSetting,
    });
    refreshData(req);

    const ss: SortSetting = {
      ascending: false,
      columnTitle: getSortColumn(localReqSortSetting),
    };
    onChangeSortSetting(ss);
  }, [
    propsLimit,
    localLimit,
    propsReqSortSetting,
    localReqSortSetting,
    propsTimeScale,
    localTimeScale,
    propsOnChangeLimit,
    propsOnChangeReqSort,
    onTimeScaleChange,
    onApplySearchCriteria,
    onRequestTimeChange,
    refreshData,
    onChangeSortSetting,
  ]);

  // Track when we need to call updateRequestParams after sort setting change
  const pendingUpdateRef = useRef(false);
  const prevLocalReqSortSettingRef = useRef(localReqSortSetting);

  useEffect(() => {
    if (
      pendingUpdateRef.current &&
      prevLocalReqSortSettingRef.current !== localReqSortSetting
    ) {
      pendingUpdateRef.current = false;
      updateRequestParams();
    }
    prevLocalReqSortSettingRef.current = localReqSortSetting;
  }, [localReqSortSetting, updateRequestParams]);

  const onUpdateSortSettingAndApplyWithPending = useCallback((): void => {
    const newReqSort = getReqSortColumn(sortSetting.columnTitle);
    pendingUpdateRef.current = true;
    setLocalReqSortSetting(newReqSort);
  }, [sortSetting.columnTitle]);

  const hasReqSortOption = useCallback((): boolean => {
    let found = false;
    Object.values(SqlStatsSortOptions).forEach(option => {
      const optionString = isString(option) ? option : getSortColumn(option);
      if (optionString === sortSetting.columnTitle) {
        found = true;
      }
    });
    return found;
  }, [sortSetting.columnTitle]);

  const renderTransactions = (): React.ReactElement => {
    const data = txnsResp.data;
    const internalAppNamePrefix = data?.internal_app_name_prefix || "";
    const statements = data?.statements || [];

    // We apply the search filters and app name filters prior to aggregating across Node IDs
    const { transactions: filteredTransactions, activeFilters } =
      filterTransactions(
        searchTransactionsData(search, data?.transactions || [], statements),
        filters,
        internalAppNamePrefix,
        statements,
        nodeRegions,
        isTenant,
      );

    const appNames = getTrxAppFilterOptions(
      data?.transactions || [],
      internalAppNamePrefix,
    );

    const transactionsToDisplay: TransactionInfo[] = filteredTransactions.map(
      t => ({
        stats_data: t.stats_data,
        regions: generateRegion(t, statements),
        regionNodes: generateRegionNode(t, statements, nodeRegions),
      }),
    );
    const { current, pageSize } = pagination;
    const hasData = data?.transactions?.length > 0;
    const isUsedFilter = search?.length > 0;

    const nodes = Object.keys(nodeRegions)
      .map(n => Number(n))
      .sort();

    const regions = unique(
      isTenant
        ? flatMap(statements, statement => statement.stats.regions)
        : nodes.map(node => nodeRegions[node.toString()]),
    ).sort();

    // Creates a list of all possible columns
    const columns = makeTransactionsColumns(
      transactionsToDisplay,
      statements,
      isTenant,
      search,
    )
      .filter(c => !(c.name === "regions" && regions.length < 2))
      .filter(c => !(c.name === "regionNodes" && regions.length < 2))
      .filter(c => !(isTenant && c.hideIfTenant));

    // Create list of SelectOptions
    const tableColumns = columns
      .filter(c => !c.alwaysShow)
      .map(
        (c): SelectOption => ({
          label: getLabel(c.name as StatisticTableColumnKeys, "transaction"),
          value: c.name,
          isSelected: isSelectedColumn(userSelectedColumnsToShow, c),
        }),
      );

    // List of all columns that will be displayed based on the column selection.
    const displayColumns = columns.filter(c =>
      isSelectedColumn(userSelectedColumnsToShow, c),
    );

    const sortSettingLabel = getSortLabel(propsReqSortSetting, "Transaction");
    const showSortWarning =
      !isSortSettingSameAsReqSort() &&
      hasReqSortOption() &&
      transactionsToDisplay.length === propsLimit;

    return (
      <>
        <h5 className={`${commonStyles("base-heading")} ${cx("margin-top")}`}>
          {`Results - Top ${propsLimit} Transaction Fingerprints by ${sortSettingLabel}`}
        </h5>
        <section className={cx("filter-area")}>
          <PageConfig className={cx("float-left")}>
            <PageConfigItem>
              <Search
                onSubmit={onSubmitSearchField}
                onClear={onClearSearchField}
                defaultValue={search}
              />
            </PageConfigItem>
            <PageConfigItem>
              <Filter
                onSubmitFilters={onSubmitFilters}
                appNames={appNames}
                regions={regions}
                timeLabel={"Transaction fingerprint"}
                nodes={nodes.map(n => "n" + n)}
                activeFilters={activeFilters}
                filters={filters}
                showRegions={regions.length > 1}
                showNodes={!isTenant && nodes.length > 1}
              />
            </PageConfigItem>
            <PageConfigItem>
              <ColumnsSelector
                options={tableColumns}
                onSubmitColumns={onColumnsChange}
              />
            </PageConfigItem>
          </PageConfig>
          <PageConfig className={cx("float-right")}>
            <PageConfigItem>
              <p className={timeScaleStylesCx("time-label")}>
                <TimeScaleLabel
                  timeScale={propsTimeScale}
                  requestTime={moment(requestTime)}
                  oldestDataTime={
                    oldestDataAvailable &&
                    TimestampToMoment(oldestDataAvailable)
                  }
                />
                {", "}
                <ResultsPerPageLabel
                  pagination={{
                    ...pagination,
                    total: transactionsToDisplay.length,
                  }}
                  pageName={"Transactions"}
                  search={search}
                />
              </p>
            </PageConfigItem>
            {hasAdminRole && (
              <PageConfigItem
                className={`${commonStyles("separator")} ${cx(
                  "reset-btn-area",
                )} `}
              >
                <ClearStats
                  resetSQLStats={doResetSQLStats}
                  tooltipType="transaction"
                />
              </PageConfigItem>
            )}
          </PageConfig>
        </section>
        <section className={statisticsClasses.tableContainerClass}>
          <SelectedFilters
            filters={filters}
            onRemoveFilter={onSubmitFilters}
            onClearFilters={onClearFilters}
          />
          {showSortWarning && (
            <InlineAlert
              intent="warning"
              title={getSubsetWarning(
                "transaction",
                propsLimit,
                sortSettingLabel,
                sortSetting.columnTitle as StatisticTableColumnKeys,
                onUpdateSortSettingAndApplyWithPending,
              )}
              className={cx("margin-bottom")}
            />
          )}
          <TransactionsTable
            columns={displayColumns}
            transactions={transactionsToDisplay}
            sortSetting={sortSetting}
            onChangeSortSetting={onChangeSortSetting}
            pagination={pagination}
            renderNoResult={
              <EmptyTransactionsPlaceholder
                transactionView={TransactionViewType.FINGERPRINTS}
                isEmptySearchResults={hasData && isUsedFilter}
              />
            }
          />
        </section>
        <Pagination
          pageSize={pageSize}
          current={current}
          total={transactionsToDisplay.length}
          onChange={onChangePage}
          onShowSizeChange={onChangePage}
        />
      </>
    );
  };

  const longLoadingMessage = (
    <Delayed delay={STATS_LONG_LOADING_DURATION}>
      <InlineAlert
        intent="info"
        title="If the selected time interval contains a large amount of data, this page might take a few minutes to load."
      />
    </Delayed>
  );

  return (
    <>
      <SearchCriteria
        searchType="Transaction"
        topValue={localLimit}
        byValue={localReqSortSetting}
        currentScale={localTimeScale}
        onChangeTop={onChangeLimit}
        onChangeBy={onChangeReqSort}
        onChangeTimeScale={changeTimeScale}
        onApply={updateRequestParams}
      />
      <div className={cx("table-area")}>
        <Loading
          loading={txnsResp.inFlight}
          page={"transactions"}
          error={txnsResp?.error}
          render={() => renderTransactions()}
          renderError={() =>
            LoadingError({
              statsType: "transactions",
              error: txnsResp.error,
              sourceTables: txnsResp?.data?.txns_source_table && [
                txnsResp?.data?.txns_source_table,
                txnsResp?.data?.stmts_source_table,
              ],
            })
          }
        />
        {txnsResp.inFlight && longLoadingMessage}
      </div>
    </>
  );
}
