// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import flatMap from "lodash/flatMap";
import groupBy from "lodash/groupBy";
import isString from "lodash/isString";
import merge from "lodash/merge";
import moment from "moment-timezone";
import React, { useState, useEffect, useCallback, useRef } from "react";
import { RouteComponentProps } from "react-router-dom";

import {
  SqlStatsSortType,
  StatementsRequest,
  createCombinedStmtsRequest,
  SqlStatsSortOptions,
} from "src/api/statementsApi";
import { RequestState } from "src/api/types";
import { isSelectedColumn } from "src/columnsSelector/utils";
import { Delayed } from "src/delayed";
import { getValidErrorsList, Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { Pagination, ResultsPerPageLabel } from "src/pagination";
import { Search } from "src/search";
import { SearchCriteria } from "src/searchCriteria/searchCriteria";
import {
  handleSortSettingFromQueryString,
  SortSetting,
  updateSortSettingQueryParamsOnTab,
} from "src/sortedtable";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import {
  ActivateDiagnosticsModalRef,
  ActivateStatementDiagnosticsModal,
} from "src/statementsDiagnostics";
import { TimeScaleLabel } from "src/timeScaleDropdown/timeScaleLabel";
import { Timestamp, TimestampToMoment, syncHistory, unique } from "src/util";
import {
  STATS_LONG_LOADING_DURATION,
  getSortLabel,
  getSortColumn,
  getSubsetWarning,
  getReqSortColumn,
} from "src/util/sqlActivityConstants";

import {
  InsertStmtDiagnosticRequest,
  StatementDiagnosticsReport,
  SqlStatsResponse,
  StatementDiagnosticsResponse,
} from "../api";
import ColumnsSelector from "../columnsSelector/columnsSelector";
import { commonStyles } from "../common";
import { SelectOption } from "../multiSelectCheckbox/multiSelectCheckbox";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  Filters,
  handleFiltersFromQueryString,
  SelectedFilters,
  updateFiltersQueryParamsOnTab,
} from "../queryFilter";
import { ISortedTablePagination } from "../sortedtable";
import ClearStats from "../sqlActivity/clearStats";
import LoadingError from "../sqlActivity/errorComponent";
import {
  filterStatementsData,
  convertRawStmtsToAggregateStatisticsMemoized,
  getAppsFromStmtsResponseMemoized,
} from "../sqlActivity/util";
import {
  AggregateStatistics,
  makeStatementsColumns,
  populateRegionNodeForStatements,
  StatementsSortedTable,
} from "../statementsTable";
import {
  getLabel,
  StatisticTableColumnKeys,
} from "../statsTableUtil/statsTableUtil";
import { UIConfigState } from "../store";
import {
  getValidOption,
  TimeScale,
  timeScale1hMinOptions,
  toRoundedDateRange,
} from "../timeScaleDropdown";
import timeScaleStyles from "../timeScaleDropdown/timeScale.module.scss";

import { EmptyStatementsPlaceholder } from "./emptyStatementsPlaceholder";
import { StatementViewType } from "./statementPageTypes";
import styles from "./statementsPage.module.scss";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);
const timeScaleStylesCx = classNames.bind(timeScaleStyles);

// Most of the props are supposed to be provided as connected props
// from redux store.
// StatementsPageDispatchProps, StatementsPageStateProps, and StatementsPageOuterProps interfaces
// provide convenient definitions for `mapDispatchToProps`, `mapStateToProps` and props that
// have to be provided by parent component.
export interface StatementsPageDispatchProps {
  refreshDatabases: (timeout?: moment.Duration) => void;
  refreshStatements: (req: StatementsRequest) => void;
  refreshStatementDiagnosticsRequests: () => void;
  refreshNodes: () => void;
  refreshUserSQLRoles: () => void;
  resetSQLStats: () => void;
  dismissAlertMessage: () => void;
  onActivateStatementDiagnostics: (
    insertStmtDiagnosticsRequest: InsertStmtDiagnosticRequest,
  ) => void;
  onDiagnosticsModalOpen?: (statement: string) => void;
  onSearchComplete?: (query: string) => void;
  onPageChanged?: (newPage: number) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onSelectDiagnosticsReportDropdownOption?: (
    report: StatementDiagnosticsReport,
  ) => void;
  onFilterChange?: (value: Filters) => void;
  onStatementClick?: (statement: string) => void;
  onColumnsChange?: (selectedColumns: string[]) => void;
  onTimeScaleChange: (ts: TimeScale) => void;
  onChangeLimit: (limit: number) => void;
  onChangeReqSort: (sort: SqlStatsSortType) => void;
  onApplySearchCriteria: (ts: TimeScale, limit: number, sort: string) => void;
  onRequestTimeChange: (t: moment.Moment) => void;
}
export interface StatementsPageStateProps {
  statementsResponse: RequestState<SqlStatsResponse>;
  timeScale: TimeScale;
  limit: number;
  reqSortSetting: SqlStatsSortType;
  databases: string[];
  columns: string[];
  nodeRegions: { [key: string]: string };
  sortSetting: SortSetting;
  filters: Filters;
  search: string;
  isTenant?: UIConfigState["isTenant"];
  hasViewActivityRedactedRole?: UIConfigState["hasViewActivityRedactedRole"];
  hasAdminRole?: UIConfigState["hasAdminRole"];
  stmtsTotalRuntimeSecs: number;
  statementDiagnostics: StatementDiagnosticsResponse | null;
  requestTime: moment.Moment;
  oldestDataAvailable: Timestamp;
}

export interface StatementsPageState {
  pagination: ISortedTablePagination;
  filters?: Filters;
  activeFilters?: number;
  timeScale: TimeScale;
  limit: number;
  reqSortSetting: SqlStatsSortType;
}

export type StatementsPageProps = StatementsPageDispatchProps &
  StatementsPageStateProps &
  RouteComponentProps<unknown>;

type RequestParams = Pick<
  StatementsPageState,
  "limit" | "reqSortSetting" | "timeScale"
>;

function stmtsRequestFromParams(params: RequestParams): StatementsRequest {
  const [start, end] = toRoundedDateRange(params.timeScale);
  return createCombinedStmtsRequest({
    start,
    end,
    limit: params.limit,
    sort: params.reqSortSetting,
  });
}

export function StatementsPage(props: StatementsPageProps): React.ReactElement {
  const {
    history,
    search,
    sortSetting,
    filters: propsFilters,
    onSearchComplete,
    onFilterChange,
    onSortingChange,
    refreshDatabases,
    refreshStatements,
    refreshStatementDiagnosticsRequests,
    refreshNodes,
    refreshUserSQLRoles,
    resetSQLStats: resetSQLStatsAction,
    dismissAlertMessage,
    onActivateStatementDiagnostics,
    onDiagnosticsModalOpen,
    onPageChanged,
    onSelectDiagnosticsReportDropdownOption,
    onStatementClick,
    columns: userSelectedColumnsToShow,
    onColumnsChange,
    onTimeScaleChange,
    onChangeLimit: onChangeLimitProp,
    onChangeReqSort: onChangeReqSortProp,
    onApplySearchCriteria,
    onRequestTimeChange,
    statementsResponse,
    timeScale: propsTimeScale,
    limit: propsLimit,
    reqSortSetting: propsReqSortSetting,
    databases,
    nodeRegions,
    isTenant,
    hasViewActivityRedactedRole,
    hasAdminRole,
    stmtsTotalRuntimeSecs,
    statementDiagnostics,
    requestTime,
    oldestDataAvailable,
  } = props;

  const activateDiagnosticsRef = useRef<ActivateDiagnosticsModalRef>(null);

  // Compute initial state from history once
  const getInitialState = useCallback((): StatementsPageState => {
    const defaultState: StatementsPageState = {
      pagination: {
        pageSize: 50,
        current: 1,
      },
      limit: propsLimit,
      timeScale: propsTimeScale,
      reqSortSetting: propsReqSortSetting,
    };

    const searchParams = new URLSearchParams(history.location.search);

    // Search query.
    const searchQuery = searchParams.get("q") || undefined;
    if (onSearchComplete && searchQuery && search !== searchQuery) {
      onSearchComplete(searchQuery);
    }

    // Sort Settings.
    handleSortSettingFromQueryString(
      "Statements",
      history.location.search,
      sortSetting,
      onSortingChange,
    );

    // Filters.
    const latestFilter = handleFiltersFromQueryString(
      history,
      propsFilters,
      onFilterChange,
    );

    const stateFromHistory: Partial<StatementsPageState> = {
      filters: latestFilter,
      activeFilters: calculateActiveFilters(latestFilter),
    };

    return merge(defaultState, stateFromHistory);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const [pagination, setPagination] = useState<ISortedTablePagination>(
    () => getInitialState().pagination,
  );
  const [filters, setFilters] = useState<Filters | undefined>(
    () => getInitialState().filters,
  );
  const [activeFilters, setActiveFilters] = useState<number | undefined>(
    () => getInitialState().activeFilters,
  );
  const [localTimeScale, setLocalTimeScale] = useState<TimeScale>(
    () => getInitialState().timeScale,
  );
  const [localLimit, setLocalLimit] = useState<number>(
    () => getInitialState().limit,
  );
  const [localReqSortSetting, setLocalReqSortSetting] =
    useState<SqlStatsSortType>(() => getInitialState().reqSortSetting);

  // Track pending update for setState callback pattern
  const pendingUpdateRef = useRef<(() => void) | null>(null);

  const resetPagination = useCallback((): void => {
    setPagination(prev => ({
      current: 1,
      pageSize: prev.pageSize,
    }));
  }, []);

  const refreshStatementsInternal = useCallback((): void => {
    const req = stmtsRequestFromParams({
      limit: localLimit,
      reqSortSetting: localReqSortSetting,
      timeScale: localTimeScale,
    });
    refreshStatements(req);
  }, [localLimit, localReqSortSetting, localTimeScale, refreshStatements]);

  const changeSortSetting = useCallback(
    (ss: SortSetting): void => {
      syncHistory(
        {
          ascending: ss.ascending.toString(),
          columnTitle: ss.columnTitle,
        },
        history,
      );
      if (onSortingChange) {
        onSortingChange("Statements", ss.columnTitle, ss.ascending);
      }
    },
    [history, onSortingChange],
  );

  const isSortSettingSameAsReqSort = useCallback((): boolean => {
    return getSortColumn(propsReqSortSetting) === sortSetting.columnTitle;
  }, [propsReqSortSetting, sortSetting.columnTitle]);

  const changeTimeScale = useCallback((ts: TimeScale): void => {
    setLocalTimeScale(ts);
  }, []);

  const updateRequestParams = useCallback((): void => {
    if (propsLimit !== localLimit) {
      onChangeLimitProp(localLimit);
    }

    if (propsReqSortSetting !== localReqSortSetting) {
      onChangeReqSortProp(localReqSortSetting);
    }

    if (propsTimeScale !== localTimeScale) {
      onTimeScaleChange(localTimeScale);
    }
    if (onApplySearchCriteria) {
      onApplySearchCriteria(
        localTimeScale,
        localLimit,
        getSortLabel(localReqSortSetting, "Statement"),
      );
    }
    onRequestTimeChange(moment());

    // Refresh statements with the new params
    const req = stmtsRequestFromParams({
      limit: localLimit,
      reqSortSetting: localReqSortSetting,
      timeScale: localTimeScale,
    });
    refreshStatements(req);

    const ss: SortSetting = {
      ascending: false,
      columnTitle: getSortColumn(localReqSortSetting),
    };
    changeSortSetting(ss);
  }, [
    propsLimit,
    localLimit,
    propsReqSortSetting,
    localReqSortSetting,
    propsTimeScale,
    localTimeScale,
    onChangeLimitProp,
    onChangeReqSortProp,
    onTimeScaleChange,
    onApplySearchCriteria,
    onRequestTimeChange,
    refreshStatements,
    changeSortSetting,
  ]);

  const onUpdateSortSettingAndApply = useCallback((): void => {
    setLocalReqSortSetting(getReqSortColumn(sortSetting.columnTitle));
    pendingUpdateRef.current = updateRequestParams;
  }, [sortSetting.columnTitle, updateRequestParams]);

  // Execute pending update after state changes
  useEffect(() => {
    if (pendingUpdateRef.current) {
      pendingUpdateRef.current();
      pendingUpdateRef.current = null;
    }
  }, [localReqSortSetting]);

  const refreshDatabasesInternal = useCallback((): void => {
    refreshDatabases();
  }, [refreshDatabases]);

  const resetSQLStats = useCallback((): void => {
    resetSQLStatsAction();
  }, [resetSQLStatsAction]);

  const updateQueryParams = useCallback((): void => {
    const tab = "Statements";

    // Search.
    const searchParams = new URLSearchParams(history.location.search);
    const currentTab = searchParams.get("tab") || "";
    const searchQueryString = searchParams.get("q") || "";
    if (currentTab === tab && search && search !== searchQueryString) {
      syncHistory(
        {
          q: search,
        },
        history,
      );
    }

    // Filters.
    updateFiltersQueryParamsOnTab(tab, filters, history);

    // Sort Setting.
    updateSortSettingQueryParamsOnTab(
      tab,
      sortSetting,
      {
        ascending: false,
        columnTitle: "executionCount",
      },
      history,
    );
  }, [history, search, filters, sortSetting]);

  const onChangePage = useCallback(
    (current: number, pageSize: number): void => {
      setPagination(prev => ({ ...prev, current, pageSize }));
      if (onPageChanged) {
        onPageChanged(current);
      }
    },
    [onPageChanged],
  );

  const onSubmitSearchField = useCallback(
    (searchValue: string): void => {
      if (onSearchComplete) {
        onSearchComplete(searchValue);
      }
      resetPagination();
      syncHistory(
        {
          q: searchValue,
        },
        history,
      );
    },
    [onSearchComplete, resetPagination, history],
  );

  const onSubmitFilters = useCallback(
    (newFilters: Filters): void => {
      if (onFilterChange) {
        onFilterChange(newFilters);
      }

      setFilters(newFilters);
      setActiveFilters(calculateActiveFilters(newFilters));

      resetPagination();
      syncHistory(
        {
          app: newFilters.app,
          timeNumber: newFilters.timeNumber,
          timeUnit: newFilters.timeUnit,
          fullScan: newFilters.fullScan.toString(),
          sqlType: newFilters.sqlType,
          database: newFilters.database,
          regions: newFilters.regions,
          nodes: newFilters.nodes,
        },
        history,
      );
    },
    [onFilterChange, resetPagination, history],
  );

  const onClearSearchField = useCallback((): void => {
    if (onSearchComplete) {
      onSearchComplete("");
    }
    syncHistory(
      {
        q: undefined,
      },
      history,
    );
  }, [onSearchComplete, history]);

  const onClearFilters = useCallback((): void => {
    if (onFilterChange) {
      onFilterChange(defaultFilters);
    }

    setFilters(defaultFilters);
    setActiveFilters(0);

    resetPagination();
    syncHistory(
      {
        app: undefined,
        timeNumber: undefined,
        timeUnit: undefined,
        fullScan: undefined,
        sqlType: undefined,
        database: undefined,
        regions: undefined,
        nodes: undefined,
      },
      history,
    );
  }, [onFilterChange, resetPagination, history]);

  const onChangeLimit = useCallback((newLimit: number): void => {
    setLocalLimit(newLimit);
  }, []);

  const onChangeReqSort = useCallback((newSort: SqlStatsSortType): void => {
    setLocalReqSortSetting(newSort);
  }, []);

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

  // componentDidMount
  useEffect(() => {
    refreshDatabasesInternal();
    // In case the user selected a option not available on this page,
    // force a selection of a valid option. This is necessary for the case
    // where the value 10/30 min is selected on the Metrics page.
    const ts = getValidOption(propsTimeScale, timeScale1hMinOptions);
    if (ts !== propsTimeScale) {
      changeTimeScale(ts);
    } else if (
      !statementsResponse.valid ||
      !statementsResponse.data ||
      !statementsResponse.lastUpdated
    ) {
      refreshStatementsInternal();
    }

    refreshUserSQLRoles();
    if (!isTenant) {
      refreshNodes();
    }
    if (!hasViewActivityRedactedRole) {
      refreshStatementDiagnosticsRequests();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // componentDidUpdate - update query params and refresh data
  useEffect(() => {
    updateQueryParams();
    if (!isTenant) {
      refreshNodes();
    }
    if (!hasViewActivityRedactedRole) {
      refreshStatementDiagnosticsRequests();
    }
  }, [
    updateQueryParams,
    isTenant,
    refreshNodes,
    hasViewActivityRedactedRole,
    refreshStatementDiagnosticsRequests,
  ]);

  // componentWillUnmount
  useEffect(() => {
    return () => {
      dismissAlertMessage();
    };
  }, [dismissAlertMessage]);

  const renderStatements = useCallback(
    (statements: AggregateStatistics[]): React.ReactElement => {
      const data = filterStatementsData(filters, search, statements, isTenant);

      const apps = getAppsFromStmtsResponseMemoized(statementsResponse?.data);

      const isEmptySearchResults = statements?.length > 0 && search?.length > 0;
      const nodes = Object.keys(nodeRegions)
        .map(n => Number(n))
        .sort();
      const regions = unique(
        isTenant
          ? flatMap(statements, statement => statement.stats.regions)
          : nodes.map(node => nodeRegions[node.toString()]),
      ).sort();

      // If the cluster is a tenant cluster we don't show info
      // about nodes/regions.
      populateRegionNodeForStatements(statements, nodeRegions);

      // Creates a list of all possible columns,
      // hiding nodeRegions if is not multi-region and
      // hiding columns that won't be displayed for virtual clusters.
      const columns = makeStatementsColumns(
        statements,
        filters?.app?.split(","),
        stmtsTotalRuntimeSecs,
        "statement",
        isTenant,
        hasViewActivityRedactedRole,
        search,
        activateDiagnosticsRef,
        onSelectDiagnosticsReportDropdownOption,
        onStatementClick,
      )
        .filter(c => !(c.name === "regions" && regions.length < 2))
        .filter(c => !(c.name === "regionNodes" && regions.length < 2))
        .filter(c => !(isTenant && c.hideIfTenant));

      // Iterate over all available columns and create list of SelectOptions with initial selection
      // values based on stored user selections in local storage and default column configs.
      // Columns that are set to alwaysShow are filtered from the list.
      const tableColumns = columns
        .filter(c => !c.alwaysShow)
        .map(
          (c): SelectOption => ({
            label: getLabel(c.name as StatisticTableColumnKeys, "statement"),
            value: c.name,
            isSelected: isSelectedColumn(userSelectedColumnsToShow, c),
          }),
        );

      // List of all columns that will be displayed based on the column selection.
      const displayColumns = columns.filter(c =>
        isSelectedColumn(userSelectedColumnsToShow, c),
      );

      const sortSettingLabel = getSortLabel(propsReqSortSetting, "Statement");
      const showSortWarning =
        !isSortSettingSameAsReqSort() &&
        hasReqSortOption() &&
        data.length === propsLimit;

      return (
        <>
          <h5 className={`${commonStyles("base-heading")} ${cx("margin-top")}`}>
            {`Results - Top ${propsLimit} Statement Fingerprints by ${sortSettingLabel}`}
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
                  appNames={apps}
                  dbNames={databases}
                  regions={regions}
                  nodes={nodes.map(n => "n" + n)}
                  activeFilters={activeFilters}
                  filters={filters}
                  showDB={true}
                  showSqlType={true}
                  showScan={true}
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
                    pagination={{ ...pagination, total: data.length }}
                    pageName={"Statements"}
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
                    resetSQLStats={resetSQLStats}
                    tooltipType="statement"
                  />
                </PageConfigItem>
              )}
            </PageConfig>
          </section>
          <section className={sortableTableCx("cl-table-container")}>
            <SelectedFilters
              filters={filters}
              onRemoveFilter={onSubmitFilters}
              onClearFilters={onClearFilters}
            />
            {showSortWarning && (
              <InlineAlert
                intent="warning"
                title={getSubsetWarning(
                  "statement",
                  propsLimit,
                  sortSettingLabel,
                  sortSetting.columnTitle as StatisticTableColumnKeys,
                  onUpdateSortSettingAndApply,
                )}
                className={cx("margin-bottom")}
              />
            )}
            <StatementsSortedTable
              className="statements-table"
              data={data}
              columns={displayColumns}
              sortSetting={sortSetting}
              onChangeSortSetting={changeSortSetting}
              renderNoResult={
                <EmptyStatementsPlaceholder
                  isEmptySearchResults={isEmptySearchResults}
                  statementView={StatementViewType.FINGERPRINTS}
                />
              }
              pagination={pagination}
            />
          </section>
          <Pagination
            pageSize={pagination.pageSize}
            current={pagination.current}
            total={data.length}
            onChange={onChangePage}
            onShowSizeChange={onChangePage}
          />
        </>
      );
    },
    [
      filters,
      search,
      isTenant,
      statementsResponse?.data,
      nodeRegions,
      stmtsTotalRuntimeSecs,
      hasViewActivityRedactedRole,
      onSelectDiagnosticsReportDropdownOption,
      onStatementClick,
      userSelectedColumnsToShow,
      propsReqSortSetting,
      isSortSettingSameAsReqSort,
      hasReqSortOption,
      propsLimit,
      onSubmitSearchField,
      onClearSearchField,
      onSubmitFilters,
      databases,
      activeFilters,
      onColumnsChange,
      propsTimeScale,
      requestTime,
      oldestDataAvailable,
      pagination,
      hasAdminRole,
      resetSQLStats,
      onClearFilters,
      sortSetting,
      onUpdateSortSettingAndApply,
      changeSortSetting,
      onChangePage,
    ],
  );

  const diagnosticsByStatement = groupBy(
    statementDiagnostics,
    sd => sd.statement_fingerprint,
  );

  const statements = convertRawStmtsToAggregateStatisticsMemoized(
    statementsResponse?.data?.statements,
  ).map(
    (s): AggregateStatistics => ({
      ...s,
      diagnosticsReports: diagnosticsByStatement[s.label] || [],
    }),
  );

  const longLoadingMessage = (
    <Delayed delay={STATS_LONG_LOADING_DURATION}>
      <InlineAlert
        intent="info"
        title="If the selected time interval contains a large amount of data, this page might take a few minutes to load."
      />
    </Delayed>
  );

  return (
    <div className={cx("root")}>
      <SearchCriteria
        searchType="Statement"
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
          loading={statementsResponse.inFlight}
          page={"statements"}
          error={statementsResponse.error}
          render={() => renderStatements(statements)}
          renderError={() =>
            LoadingError({
              statsType: "statements",
              error: statementsResponse?.error,
              sourceTables: statementsResponse?.data?.stmts_source_table && [
                statementsResponse?.data?.stmts_source_table,
              ],
            })
          }
        />
        {statementsResponse.inFlight &&
          getValidErrorsList(statementsResponse.error) == null &&
          longLoadingMessage}
        <ActivateStatementDiagnosticsModal
          ref={activateDiagnosticsRef}
          activate={onActivateStatementDiagnostics}
          refreshDiagnosticsReports={refreshStatementDiagnosticsRequests}
          onOpenModal={onDiagnosticsModalOpen}
        />
      </div>
    </div>
  );
}
