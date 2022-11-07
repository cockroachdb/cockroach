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
import { isNil, merge } from "lodash";
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
import { Pagination } from "src/pagination";
import { TableStatistics } from "../tableStatistics";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  Filters,
  handleFiltersFromQueryString,
  updateFiltersQueryParamsOnTab,
} from "../queryFilter";

import { calculateTotalWorkload, syncHistory, unique } from "src/util";
import {
  addInsightCounts,
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
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { InlineAlert } from "@cockroachlabs/ui-components";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import ColumnsSelector from "../columnsSelector/columnsSelector";
import { SelectOption } from "../multiSelectCheckbox/multiSelectCheckbox";
import { UIConfigState } from "../store";
import { StatementsRequest } from "src/api/statementsApi";
import Long from "long";
import ClearStats from "../sqlActivity/clearStats";
import LoadingError from "../sqlActivity/errorComponent";
import {
  getValidOption,
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
  timeScaleRangeToObj,
  timeScaleToString,
  toRoundedDateRange,
} from "../timeScaleDropdown";

import { commonStyles } from "../common";
import { isSelectedColumn } from "src/columnsSelector/utils";
import { StatementViewType } from "./statementPageTypes";
import moment from "moment";
import {
  InsertStmtDiagnosticRequest,
  StatementDiagnosticsReport,
  StmtInsightsReq,
} from "../api";
import { filteredStatementsData } from "../sqlActivity/util";
import { ExecutionInsightCountEvent } from "../insights";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

const POLLING_INTERVAL_MILLIS = 300000;

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
  resetSQLStats: (req: StatementsRequest) => void;
  refreshInsightCount: (req: StmtInsightsReq) => void;
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
}

export interface StatementsPageStateProps {
  statements: AggregateStatistics[];
  isDataValid: boolean;
  lastUpdated: moment.Moment | null;
  timeScale: TimeScale;
  statementsError: Error | null;
  apps: string[];
  databases: string[];
  totalFingerprints: number;
  lastReset: string;
  columns: string[];
  nodeRegions: { [key: string]: string };
  sortSetting: SortSetting;
  filters: Filters;
  search: string;
  insightCounts: ExecutionInsightCountEvent[];
  isTenant?: UIConfigState["isTenant"];
  hasViewActivityRedactedRole?: UIConfigState["hasViewActivityRedactedRole"];
  hasAdminRole?: UIConfigState["hasAdminRole"];
}

export interface StatementsPageState {
  pagination: ISortedTablePagination;
  filters?: Filters;
  activeFilters?: number;
  startRequest?: Date;
}

export type StatementsPageProps = StatementsPageDispatchProps &
  StatementsPageStateProps &
  RouteComponentProps<unknown>;

function stmtsRequestFromTimeScale(
  ts: TimeScale,
): cockroach.server.serverpb.StatementsRequest {
  const [start, end] = toRoundedDateRange(ts);
  return new cockroach.server.serverpb.StatementsRequest({
    combined: true,
    start: Long.fromNumber(start.unix()),
    end: Long.fromNumber(end.unix()),
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
  refreshDataTimeout: NodeJS.Timeout;

  constructor(props: StatementsPageProps) {
    super(props);
    const defaultState = {
      pagination: {
        pageSize: 20,
        current: 1,
      },
      startRequest: new Date(),
    };
    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(defaultState, stateFromHistory);
    this.activateDiagnosticsRef = React.createRef();

    // In case the user selected a option not available on this page,
    // force a selection of a valid option. This is necessary for the case
    // where the value 10/30 min is selected on the Metrics page.
    const ts = getValidOption(this.props.timeScale, timeScale1hMinOptions);
    if (ts !== this.props.timeScale) {
      this.changeTimeScale(ts);
    }
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

  changeTimeScale = (ts: TimeScale): void => {
    if (this.props.onTimeScaleChange) {
      this.props.onTimeScaleChange(ts);
    }
    this.refreshStatements(ts);
    this.setState({
      startRequest: new Date(),
    });
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

  clearRefreshDataTimeout(): void {
    if (this.refreshDataTimeout != null) {
      clearTimeout(this.refreshDataTimeout);
    }
  }

  resetPolling(ts: TimeScale): void {
    this.clearRefreshDataTimeout();
    if (ts.key !== "Custom") {
      this.refreshDataTimeout = setTimeout(
        this.refreshStatements,
        POLLING_INTERVAL_MILLIS, // 5 minutes
        ts,
      );
    }
  }

  refreshStatements = (ts?: TimeScale): void => {
    const time = ts ?? this.props.timeScale;
    const insightCountReq = timeScaleRangeToObj(time);
    this.props.refreshInsightCount(insightCountReq);
    const req = stmtsRequestFromTimeScale(time);
    this.props.refreshStatements(req);
    this.resetPolling(time);
  };

  refreshDatabases = (): void => {
    this.props.refreshDatabases();
  };

  resetSQLStats = (): void => {
    const req = stmtsRequestFromTimeScale(this.props.timeScale);
    this.props.resetSQLStats(req);
    this.setState({
      startRequest: new Date(),
    });
  };

  componentDidMount(): void {
    this.setState({
      startRequest: new Date(),
    });

    // For the first data fetch for this page, we refresh immediately if:
    // - Last updated is null (no statements fetched previously)
    // - The data is not valid (time scale may have changed on other pages)
    // - The time range selected is a moving window and the last udpated time
    // is >= 5 minutes.
    // Otherwise, we schedule a refresh at 5 mins from the lastUpdated time if
    // the time range selected is a moving window (i.e. not custom).
    const now = moment();
    let nextRefresh = null;
    if (this.props.lastUpdated == null || !this.props.isDataValid) {
      nextRefresh = now;
    } else if (this.props.timeScale.key !== "Custom") {
      nextRefresh = this.props.lastUpdated
        .clone()
        .add(POLLING_INTERVAL_MILLIS, "milliseconds");
    }
    if (nextRefresh) {
      this.refreshDataTimeout = setTimeout(
        this.refreshStatements,
        Math.max(0, nextRefresh.diff(now, "milliseconds")),
      );
    }

    if (!this.props.insightCounts) {
      const insightCountReq = timeScaleRangeToObj(this.props.timeScale);
      this.props.refreshInsightCount(insightCountReq);
    }

    this.refreshDatabases();

    this.props.refreshUserSQLRoles();
    this.props.refreshNodes();
    if (!this.props.isTenant && !this.props.hasViewActivityRedactedRole) {
      this.props.refreshStatementDiagnosticsRequests();
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
    this.props.refreshNodes();
    if (!this.props.isTenant && !this.props.hasViewActivityRedactedRole) {
      this.props.refreshStatementDiagnosticsRequests();
    }
  };

  componentWillUnmount(): void {
    this.props.dismissAlertMessage();
    this.clearRefreshDataTimeout();
  }

  onChangePage = (current: number): void => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
    this.props.onPageChanged(current);
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

  renderStatements = (regions: string[]): React.ReactElement => {
    const { pagination, filters, activeFilters } = this.state;
    const {
      statements,
      onSelectDiagnosticsReportDropdownOption,
      onStatementClick,
      columns: userSelectedColumnsToShow,
      onColumnsChange,
      nodeRegions,
      isTenant,
      hasViewActivityRedactedRole,
      sortSetting,
      search,
      insightCounts,
    } = this.props;
    const stmts = filteredStatementsData(
      filters,
      search,
      statements,
      nodeRegions,
      isTenant,
    );
    const data = addInsightCounts(stmts, insightCounts);
    const totalWorkload = calculateTotalWorkload(statements);
    const totalCount = data.length;
    const isEmptySearchResults = statements?.length > 0 && search?.length > 0;
    // If the cluster is a tenant cluster we don't show info
    // about nodes/regions.
    populateRegionNodeForStatements(statements, nodeRegions);

    // Creates a list of all possible columns,
    // hiding nodeRegions if is not multi-region and
    // hiding columns that won't be displayed for tenants.
    const columns = makeStatementsColumns(
      statements,
      filters.app.split(","),
      totalWorkload,
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

    const period = timeScaleToString(this.props.timeScale);

    return (
      <div>
        <section className={sortableTableCx("cl-table-container")}>
          <div>
            <ColumnsSelector
              options={tableColumns}
              onSubmitColumns={onColumnsChange}
            />
            <TableStatistics
              pagination={pagination}
              search={search}
              totalCount={totalCount}
              arrayItemName="statements"
              activeFilters={activeFilters}
              period={period}
              onClearFilters={this.onClearFilters}
            />
          </div>
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
      </div>
    );
  };

  render(): React.ReactElement {
    const {
      refreshStatementDiagnosticsRequests,
      onActivateStatementDiagnostics,
      onDiagnosticsModalOpen,
      apps,
      databases,
      search,
      isTenant,
      nodeRegions,
      hasAdminRole,
    } = this.props;

    const nodes = Object.keys(nodeRegions)
      .map(n => Number(n))
      .sort();

    const regions = unique(
      nodes.map(node => nodeRegions[node.toString()]),
    ).sort();

    const { filters, activeFilters } = this.state;

    const longLoadingMessage = isNil(this.props.statements) &&
      isNil(getValidErrorsList(this.props.statementsError)) && (
        <Delayed delay={moment.duration(2, "s")}>
          <InlineAlert
            intent="info"
            title="If the selected time interval contains a large amount of data, this page might take a few minutes to load."
          />
        </Delayed>
      );

    return (
      <div className={cx("root")}>
        <PageConfig>
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
          <PageConfigItem className={commonStyles("separator")}>
            <TimeScaleDropdown
              options={timeScale1hMinOptions}
              currentScale={this.props.timeScale}
              setTimeScale={this.changeTimeScale}
            />
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
        <div className={cx("table-area")}>
          <Loading
            loading={isNil(this.props.statements)}
            page={"statements"}
            error={this.props.statementsError}
            render={() => this.renderStatements(regions)}
            renderError={() =>
              LoadingError({
                statsType: "statements",
                timeout: this.props.statementsError?.name
                  ?.toLowerCase()
                  .includes("timeout"),
              })
            }
          />
          {longLoadingMessage}
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
