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
import React, {
  useContext,
  useState,
  useEffect,
  useCallback,
  useRef,
} from "react";
import { RouteComponentProps } from "react-router-dom";

import { useDatabasesList } from "src/api/databasesApi";
import { useNodes } from "src/api/nodesApi";
import { useResetSQLStats } from "src/api/sqlStatsApi";
import {
  useCancelDiagnosticsReport,
  useCreateDiagnosticsReport,
  useStatementDiagnostics,
} from "src/api/statementDiagnosticsApi";
import {
  SqlStatsSortType,
  SqlStatsSortOptions,
  useCombinedStatementStats,
} from "src/api/statementsApi";
import { useUserSQLRoles } from "src/api/userApi";
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
import { TimestampToMoment, syncHistory, unique } from "src/util";
import {
  STATS_LONG_LOADING_DURATION,
  getSortLabel,
  getSortColumn,
  getSubsetWarning,
  getReqSortColumn,
} from "src/util/sqlActivityConstants";

import { StatementDiagnosticsReport } from "../api";
import ColumnsSelector from "../columnsSelector/columnsSelector";
import { commonStyles } from "../common";
import { ClusterDetailsContext } from "../contexts";
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
import {
  getValidOption,
  TimeScale,
  timeScale1hMinOptions,
} from "../timeScaleDropdown";
import timeScaleStyles from "../timeScaleDropdown/timeScale.module.scss";

import { EmptyStatementsPlaceholder } from "./emptyStatementsPlaceholder";
import { StatementViewType } from "./statementPageTypes";
import styles from "./statementsPage.module.scss";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);
const timeScaleStylesCx = classNames.bind(timeScaleStyles);

// StatementsPageDispatchProps defines callbacks for analytics tracking
// and UI state updates, provided by the connected wrapper.
// Data fetching is handled internally via SWR hooks.
export interface StatementsPageDispatchProps {
  onActivateStatementDiagnosticsAnalytics?: (
    statementFingerprint: string,
  ) => void;
  onDiagnosticsModalOpenAnalytics?: (statement: string) => void;
  onSearchComplete?: (query: string) => void;
  onPageChanged?: (newPage: number) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onDiagnosticsReportDropdownOptionAnalytics?: (
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
  timeScale: TimeScale;
  limit: number;
  reqSortSetting: SqlStatsSortType;
  columns: string[];
  sortSetting: SortSetting;
  filters: Filters;
  search: string;
  requestTime: moment.Moment;
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

export function StatementsPage(props: StatementsPageProps): React.ReactElement {
  const {
    history,
    search,
    sortSetting,
    filters: propsFilters,
    onSearchComplete,
    onFilterChange,
    onSortingChange,
    onActivateStatementDiagnosticsAnalytics,
    onDiagnosticsModalOpenAnalytics,
    onPageChanged,
    onDiagnosticsReportDropdownOptionAnalytics,
    onStatementClick,
    columns: userSelectedColumnsToShow,
    onColumnsChange,
    onTimeScaleChange,
    onChangeLimit: onChangeLimitProp,
    onChangeReqSort: onChangeReqSortProp,
    onApplySearchCriteria,
    onRequestTimeChange,
    timeScale: propsTimeScale,
    limit: propsLimit,
    reqSortSetting: propsReqSortSetting,
    requestTime,
  } = props;

  const { isTenant } = useContext(ClusterDetailsContext);

  const {
    data: statementsData,
    error: statementsError,
    isLoading: statementsLoading,
  } = useCombinedStatementStats(
    propsTimeScale,
    propsLimit,
    propsReqSortSetting,
  );
  const { data: statementDiagnostics } = useStatementDiagnostics();
  const { createReport: activateStatementDiagnostics } =
    useCreateDiagnosticsReport();
  const { cancelReport: cancelStatementDiagnostic } =
    useCancelDiagnosticsReport();
  const { databases } = useDatabasesList();
  const { nodeRegionsByID: nodeRegions } = useNodes();
  const { data: userRoles } = useUserSQLRoles();
  const { reset: resetSQLStats } = useResetSQLStats();

  const hasViewActivityRedactedRole =
    userRoles?.roles?.includes("VIEWACTIVITYREDACTED") ?? false;
  const hasAdminRole = userRoles?.roles?.includes("ADMIN") ?? false;
  const stmtsTotalRuntimeSecs = statementsData?.stmts_total_runtime_secs ?? 0;
  const oldestDataAvailable = statementsData?.oldest_aggregated_ts_returned;

  const activateDiagnosticsRef = useRef<ActivateDiagnosticsModalRef>(null);

  // Ref to hold latest value for mount effect, avoiding stale closure
  // while preserving "run once on mount" semantics.
  const propsTimeScaleRef = useRef(propsTimeScale);

  // Keep ref up to date on each render.
  propsTimeScaleRef.current = propsTimeScale;

  // Compute initial state from history once
  const getInitialState = (): StatementsPageState => {
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
  };

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

  const resetPagination = (): void => {
    setPagination(prev => ({
      current: 1,
      pageSize: prev.pageSize,
    }));
  };

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

  const isSortSettingSameAsReqSort = (): boolean => {
    return getSortColumn(propsReqSortSetting) === sortSetting.columnTitle;
  };

  const changeTimeScale = (ts: TimeScale): void => {
    setLocalTimeScale(ts);
  };

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

  // Sync query params when search, filters, or sort settings change.
  useEffect(() => {
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

  const onChangePage = (current: number, pageSize: number): void => {
    setPagination(prev => ({ ...prev, current, pageSize }));
    if (onPageChanged) {
      onPageChanged(current);
    }
  };

  const onSubmitSearchField = (searchValue: string): void => {
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
  };

  const onSubmitFilters = (newFilters: Filters): void => {
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
  };

  const onClearSearchField = (): void => {
    if (onSearchComplete) {
      onSearchComplete("");
    }
    syncHistory(
      {
        q: undefined,
      },
      history,
    );
  };

  const onClearFilters = (): void => {
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
  };

  const onChangeLimit = (newLimit: number): void => {
    setLocalLimit(newLimit);
  };

  const onChangeReqSort = (newSort: SqlStatsSortType): void => {
    setLocalReqSortSetting(newSort);
  };

  const hasReqSortOption = (): boolean => {
    let found = false;
    Object.values(SqlStatsSortOptions).forEach(option => {
      const optionString = isString(option) ? option : getSortColumn(option);
      if (optionString === sortSetting.columnTitle) {
        found = true;
      }
    });
    return found;
  };

  useEffect(() => {
    // In case the user selected a option not available on this page,
    // force a selection of a valid option. This is necessary for the case
    // where the value 10/30 min is selected on the Metrics page.
    const ts = getValidOption(propsTimeScaleRef.current, timeScale1hMinOptions);
    if (ts !== propsTimeScaleRef.current) {
      changeTimeScale(ts);
    }
  }, []);

  const handleDiagnosticsDropdownOption = (
    report: StatementDiagnosticsReport,
  ): void => {
    if (!report.completed) {
      cancelStatementDiagnostic({ requestId: report.id }).catch(() => {});
    }
    onDiagnosticsReportDropdownOptionAnalytics?.(report);
  };

  const renderStatements = (
    statements: AggregateStatistics[],
  ): React.ReactElement => {
    const data = filterStatementsData(filters, search, statements, isTenant);

    const apps = getAppsFromStmtsResponseMemoized(statementsData);

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
      handleDiagnosticsDropdownOption,
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
  };

  const diagnosticsByStatement = groupBy(
    statementDiagnostics,
    sd => sd.statement_fingerprint,
  );

  const statements = convertRawStmtsToAggregateStatisticsMemoized(
    statementsData?.statements,
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
          loading={statementsLoading}
          page={"statements"}
          error={statementsError}
          render={() => renderStatements(statements)}
          renderError={() =>
            LoadingError({
              statsType: "statements",
              error: statementsError,
              sourceTables: statementsData?.stmts_source_table && [
                statementsData?.stmts_source_table,
              ],
            })
          }
        />
        {statementsLoading &&
          getValidErrorsList(statementsError) == null &&
          longLoadingMessage}
        <ActivateStatementDiagnosticsModal
          ref={activateDiagnosticsRef}
          activate={req => {
            activateStatementDiagnostics(req)
              .then(() =>
                onActivateStatementDiagnosticsAnalytics?.(req.stmtFingerprint),
              )
              .catch(() => {});
          }}
          // No-op: SWR revalidates via mutate() in createReport.
          refreshDiagnosticsReports={() => {}}
          onOpenModal={onDiagnosticsModalOpenAnalytics}
        />
      </div>
    </div>
  );
}
