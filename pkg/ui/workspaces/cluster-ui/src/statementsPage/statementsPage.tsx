// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { RouteComponentProps } from "react-router-dom";
import { flatMap, merge } from "lodash";
import classNames from "classnames/bind";
import { getValidErrorsList, Loading } from "src/loading";
import { Delayed } from "src/delayed";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import {
  handleSortSettingFromQueryString,
  SortSetting,
  updateSortSettingQueryParamsOnTab,
} from "src/sortedtable";
import { Search } from "src/search";
import { Pagination, ResultsPerPageLabel } from "src/pagination";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  Filters,
  handleFiltersFromQueryString,
  SelectedFilters,
  updateFiltersQueryParamsOnTab,
} from "../queryFilter";

import { syncHistory, unique } from "src/util";
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
  ActivateDiagnosticsModalRef,
  ActivateStatementDiagnosticsModal,
} from "src/statementsDiagnostics";
import { ISortedTablePagination } from "../sortedtable";
import styles from "./statementsPage.module.scss";
import { EmptyStatementsPlaceholder } from "./emptyStatementsPlaceholder";
import { InlineAlert } from "@cockroachlabs/ui-components";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import ColumnsSelector from "../columnsSelector/columnsSelector";
import { SelectOption } from "../multiSelectCheckbox/multiSelectCheckbox";
import { UIConfigState } from "../store";
import {
  SqlStatsSortType,
  StatementsRequest,
  createCombinedStmtsRequest,
  SqlStatsSortOptions,
} from "src/api/statementsApi";
import ClearStats from "../sqlActivity/clearStats";
import LoadingError from "../sqlActivity/errorComponent";
import {
  getValidOption,
  TimeScale,
  timeScale1hMinOptions,
  toRoundedDateRange,
} from "../timeScaleDropdown";

import { commonStyles } from "../common";
import { isSelectedColumn } from "src/columnsSelector/utils";
import { StatementViewType } from "./statementPageTypes";
import moment from "moment-timezone";
import {
  InsertStmtDiagnosticRequest,
  StatementDiagnosticsReport,
} from "../api";
import { filteredStatementsData } from "../sqlActivity/util";
import {
  STATS_LONG_LOADING_DURATION,
  stmtRequestSortOptions,
  getSortLabel,
  getSortColumn,
  getSubsetWarning,
  getReqSortColumn,
} from "src/util/sqlActivityConstants";
import { SearchCriteria } from "src/searchCriteria/searchCriteria";
import timeScaleStyles from "../timeScaleDropdown/timeScale.module.scss";
import { FormattedTimescale } from "../timeScaleDropdown/formattedTimeScale";

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
}
export interface StatementsPageStateProps {
  statements: AggregateStatistics[];
  isDataValid: boolean;
  isReqInFlight: boolean;
  lastUpdated: moment.Moment | null;
  timeScale: TimeScale;
  limit: number;
  reqSortSetting: SqlStatsSortType;
  statementsError: Error | null;
  apps: string[];
  databases: string[];
  lastReset: string;
  columns: string[];
  nodeRegions: { [key: string]: string };
  sortSetting: SortSetting;
  filters: Filters;
  search: string;
  isTenant?: UIConfigState["isTenant"];
  hasViewActivityRedactedRole?: UIConfigState["hasViewActivityRedactedRole"];
  hasAdminRole?: UIConfigState["hasAdminRole"];
  stmtsTotalRuntimeSecs: number;
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

// filterBySearchQuery returns true if a search query matches the statement.
export function filterBySearchQuery(
  statement: AggregateStatistics,
  search: string,
): boolean {
  const matchString = statement.label.toLowerCase();
  const matchFingerPrintId = statement.aggregatedFingerprintHexID;

  // If search term is wrapped by quotes, do the exact search term.
  if (search.startsWith('"') && search.endsWith('"')) {
    search = search.substring(1, search.length - 1);

    return matchString.includes(search) || matchFingerPrintId.includes(search);
  }

  return search
    .toLowerCase()
    .split(" ")
    .every(
      val => matchString.includes(val) || matchFingerPrintId.includes(val),
    );
}

export class StatementsPage extends React.Component<
  StatementsPageProps,
  StatementsPageState
> {
  activateDiagnosticsRef: React.RefObject<ActivateDiagnosticsModalRef>;

  constructor(props: StatementsPageProps) {
    super(props);
    const defaultState = {
      pagination: {
        pageSize: 50,
        current: 1,
      },
      limit: this.props.limit,
      timeScale: this.props.timeScale,
      reqSortSetting: this.props.reqSortSetting,
    };
    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(defaultState, stateFromHistory);
    this.activateDiagnosticsRef = React.createRef();
  }

  getStateFromHistory = (): Partial<StatementsPageState> => {
    const {
      history,
      search,
      sortSetting,
      filters,
      onSearchComplete,
      onFilterChange,
      onSortingChange,
    } = this.props;
    const searchParams = new URLSearchParams(history.location.search);

    // Search query.
    const searchQuery = searchParams.get("q") || undefined;
    if (onSearchComplete && searchQuery && search != searchQuery) {
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
      filters,
      onFilterChange,
    );

    return {
      filters: latestFilter,
      activeFilters: calculateActiveFilters(latestFilter),
    };
  };

  changeSortSetting = (ss: SortSetting): void => {
    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      this.props.history,
    );
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Statements", ss.columnTitle, ss.ascending);
    }
  };

  isSortSettingSameAsReqSort = (): boolean => {
    return (
      getSortColumn(this.props.reqSortSetting) ==
      this.props.sortSetting.columnTitle
    );
  };

  changeTimeScale = (ts: TimeScale): void => {
    this.setState(prevState => ({
      ...prevState,
      timeScale: ts,
    }));
  };

  updateRequestParams = (): void => {
    if (this.props.limit !== this.state.limit) {
      this.props.onChangeLimit(this.state.limit);
    }

    if (this.props.reqSortSetting !== this.state.reqSortSetting) {
      this.props.onChangeReqSort(this.state.reqSortSetting);
    }

    if (this.props.timeScale !== this.state.timeScale) {
      this.props.onTimeScaleChange(this.state.timeScale);
    }
    if (this.props.onApplySearchCriteria) {
      this.props.onApplySearchCriteria(
        this.state.timeScale,
        this.state.limit,
        getSortLabel(this.state.reqSortSetting, "Statement"),
      );
    }
    this.refreshStatements();
    const ss: SortSetting = {
      ascending: false,
      columnTitle: getSortColumn(this.state.reqSortSetting),
    };
    this.changeSortSetting(ss);
  };

  onUpdateSortSettingAndApply = (): void => {
    this.setState(
      {
        reqSortSetting: getReqSortColumn(this.props.sortSetting.columnTitle),
      },
      () => {
        this.updateRequestParams();
      },
    );
  };

  resetPagination = (): void => {
    this.setState(prevState => {
      return {
        pagination: {
          current: 1,
          pageSize: prevState.pagination.pageSize,
        },
      };
    });
  };

  refreshStatements = (): void => {
    const req = stmtsRequestFromParams(this.state);
    this.props.refreshStatements(req);
  };

  refreshDatabases = (): void => {
    this.props.refreshDatabases();
  };

  resetSQLStats = (): void => {
    this.props.resetSQLStats();
  };

  componentDidMount(): void {
    this.refreshDatabases();
    // In case the user selected a option not available on this page,
    // force a selection of a valid option. This is necessary for the case
    // where the value 10/30 min is selected on the Metrics page.
    const ts = getValidOption(this.props.timeScale, timeScale1hMinOptions);
    if (ts !== this.props.timeScale) {
      this.changeTimeScale(ts);
    } else if (
      !this.props.isDataValid ||
      !this.props.lastUpdated ||
      !this.props.statements
    ) {
      this.refreshStatements();
    }

    this.props.refreshUserSQLRoles();
    if (!this.props.isTenant) {
      this.props.refreshNodes();
      if (!this.props.hasViewActivityRedactedRole) {
        this.props.refreshStatementDiagnosticsRequests();
      }
    }
  }

  updateQueryParams(): void {
    const { history, search, sortSetting } = this.props;
    const tab = "Statements";

    // Search.
    const searchParams = new URLSearchParams(history.location.search);
    const currentTab = searchParams.get("tab") || "";
    const searchQueryString = searchParams.get("q") || "";
    if (currentTab === tab && search && search != searchQueryString) {
      syncHistory(
        {
          q: search,
        },
        history,
      );
    }

    // Filters.
    updateFiltersQueryParamsOnTab(tab, this.state.filters, history);

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
  }

  componentDidUpdate = (): void => {
    this.updateQueryParams();
    if (!this.props.isTenant) {
      this.props.refreshNodes();
      if (!this.props.hasViewActivityRedactedRole) {
        this.props.refreshStatementDiagnosticsRequests();
      }
    }
  };

  componentWillUnmount(): void {
    this.props.dismissAlertMessage();
  }

  onChangePage = (current: number): void => {
    const { pagination } = this.state;
    this.setState(prevState => ({
      ...prevState,
      pagination: { ...pagination, current },
    }));
    if (this.props.onPageChanged) {
      this.props.onPageChanged(current);
    }
  };

  onSubmitSearchField = (search: string): void => {
    if (this.props.onSearchComplete) {
      this.props.onSearchComplete(search);
    }
    this.resetPagination();
    syncHistory(
      {
        q: search,
      },
      this.props.history,
    );
  };

  onSubmitFilters = (filters: Filters): void => {
    if (this.props.onFilterChange) {
      this.props.onFilterChange(filters);
    }

    this.setState({
      filters: filters,
      activeFilters: calculateActiveFilters(filters),
    });

    this.resetPagination();
    syncHistory(
      {
        app: filters.app,
        timeNumber: filters.timeNumber,
        timeUnit: filters.timeUnit,
        fullScan: filters.fullScan.toString(),
        sqlType: filters.sqlType,
        database: filters.database,
        regions: filters.regions,
        nodes: filters.nodes,
      },
      this.props.history,
    );
  };

  onClearSearchField = (): void => {
    if (this.props.onSearchComplete) {
      this.props.onSearchComplete("");
    }
    syncHistory(
      {
        q: undefined,
      },
      this.props.history,
    );
  };

  onClearFilters = (): void => {
    if (this.props.onFilterChange) {
      this.props.onFilterChange(defaultFilters);
    }

    this.setState({
      filters: defaultFilters,
      activeFilters: 0,
    });

    this.resetPagination();
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
      this.props.history,
    );
  };

  onChangeLimit = (newLimit: number): void => {
    this.setState(prevState => ({ ...prevState, limit: newLimit }));
  };

  onChangeReqSort = (newSort: SqlStatsSortType): void => {
    this.setState(prevState => ({ ...prevState, reqSortSetting: newSort }));
  };

  hasReqSortOption = (): boolean => {
    let found = false;
    Object.values(SqlStatsSortOptions).forEach((option: SqlStatsSortType) => {
      if (getSortColumn(option) == this.props.sortSetting.columnTitle) {
        found = true;
      }
    });
    return found;
  };

  renderStatements = (): React.ReactElement => {
    const { pagination, filters, activeFilters } = this.state;
    const {
      onSelectDiagnosticsReportDropdownOption,
      onStatementClick,
      columns: userSelectedColumnsToShow,
      onColumnsChange,
      nodeRegions,
      isTenant,
      hasViewActivityRedactedRole,
      sortSetting,
      search,
      apps,
      databases,
      hasAdminRole,
    } = this.props;
    const statements = this.props.statements ?? [];
    const data = filteredStatementsData(
      filters,
      search,
      statements,
      nodeRegions,
      isTenant,
    );
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
    // hiding columns that won't be displayed for tenants.
    const columns = makeStatementsColumns(
      statements,
      filters.app?.split(","),
      this.props.stmtsTotalRuntimeSecs,
      "statement",
      isTenant,
      hasViewActivityRedactedRole,
      search,
      this.activateDiagnosticsRef,
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

    const period = <FormattedTimescale ts={this.props.timeScale} />;
    const sortSettingLabel = getSortLabel(
      this.props.reqSortSetting,
      "Statement",
    );
    const showSortWarning =
      !this.isSortSettingSameAsReqSort() &&
      this.hasReqSortOption() &&
      data.length == this.props.limit;

    return (
      <>
        <h5 className={`${commonStyles("base-heading")} ${cx("margin-top")}`}>
          {`Results - Top ${this.props.limit} Statement Fingerprints by ${sortSettingLabel}`}
        </h5>
        <section className={cx("filter-area")}>
          <PageConfig className={cx("float-left")}>
            <PageConfigItem>
              <Search
                onSubmit={this.onSubmitSearchField}
                onClear={this.onClearSearchField}
                defaultValue={search}
              />
            </PageConfigItem>
            <PageConfigItem>
              <Filter
                onSubmitFilters={this.onSubmitFilters}
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
                Showing aggregated stats from{" "}
                <span className={timeScaleStylesCx("bold")}>{period}</span>
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
                  resetSQLStats={this.resetSQLStats}
                  tooltipType="statement"
                />
              </PageConfigItem>
            )}
          </PageConfig>
        </section>
        <section className={sortableTableCx("cl-table-container")}>
          <SelectedFilters
            filters={filters}
            onRemoveFilter={this.onSubmitFilters}
            onClearFilters={this.onClearFilters}
          />
          {showSortWarning && (
            <InlineAlert
              intent="warning"
              title={getSubsetWarning(
                "statement",
                this.props.limit,
                sortSettingLabel,
                this.props.sortSetting.columnTitle as StatisticTableColumnKeys,
                this.onUpdateSortSettingAndApply,
              )}
              className={cx("margin-bottom")}
            />
          )}
          <StatementsSortedTable
            className="statements-table"
            data={data}
            columns={displayColumns}
            sortSetting={sortSetting}
            onChangeSortSetting={this.changeSortSetting}
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
          onChange={this.onChangePage}
        />
      </>
    );
  };

  render(): React.ReactElement {
    const {
      refreshStatementDiagnosticsRequests,
      onActivateStatementDiagnostics,
      onDiagnosticsModalOpen,
    } = this.props;
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
          sortOptions={stmtRequestSortOptions}
          topValue={this.state.limit}
          byValue={this.state.reqSortSetting}
          currentScale={this.state.timeScale}
          onChangeTop={this.onChangeLimit}
          onChangeBy={this.onChangeReqSort}
          onChangeTimeScale={this.changeTimeScale}
          onApply={this.updateRequestParams}
        />
        <div className={cx("table-area")}>
          <Loading
            loading={this.props.isReqInFlight}
            page={"statements"}
            error={this.props.statementsError}
            render={() => this.renderStatements()}
            renderError={() =>
              LoadingError({
                statsType: "statements",
                timeout: this.props.statementsError?.name
                  ?.toLowerCase()
                  .includes("timeout"),
              })
            }
          />
          {this.props.isReqInFlight &&
            getValidErrorsList(this.props.statementsError) == null &&
            longLoadingMessage}
          <ActivateStatementDiagnosticsModal
            ref={this.activateDiagnosticsRef}
            activate={onActivateStatementDiagnostics}
            refreshDiagnosticsReports={refreshStatementDiagnosticsRequests}
            onOpenModal={onDiagnosticsModalOpen}
          />
        </div>
      </div>
    );
  }
}
