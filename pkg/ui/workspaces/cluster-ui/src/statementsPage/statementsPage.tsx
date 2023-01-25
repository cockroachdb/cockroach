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
  getTimeValueInSeconds,
  handleFiltersFromQueryString,
  updateFiltersQueryParamsOnTab,
} from "../queryFilter";

import {
  calculateTotalWorkload,
  containAny,
  syncHistory,
  unique,
  unset,
} from "src/util";
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
import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
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
  timeScaleToString,
  toRoundedDateRange,
} from "../timeScaleDropdown";

import { commonStyles } from "../common";
import { isSelectedColumn } from "src/columnsSelector/utils";
import { StatementViewType } from "./statementPageTypes";
import moment from "moment";

type IStatementDiagnosticsReport =
  cockroach.server.serverpb.IStatementDiagnosticsReport;
type IDuration = google.protobuf.IDuration;
const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

const POLLING_INTERVAL_MILLIS = 300000;

// Most of the props are supposed to be provided as connected props
// from redux store.
// StatementsPageDispatchProps, StatementsPageStateProps, and StatementsPageOuterProps interfaces
// provide convenient definitions for `mapDispatchToProps`, `mapStateToProps` and props that
// have to be provided by parent component.
export interface StatementsPageDispatchProps {
  refreshStatements: (req: StatementsRequest) => void;
  refreshStatementDiagnosticsRequests: () => void;
  refreshNodes: () => void;
  refreshUserSQLRoles: () => void;
  resetSQLStats: (req: StatementsRequest) => void;
  dismissAlertMessage: () => void;
  onActivateStatementDiagnostics: (
    statement: string,
    minExecLatency: IDuration,
    expiresAfter: IDuration,
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
    report: IStatementDiagnosticsReport,
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
    const req = stmtsRequestFromTimeScale(time);
    this.props.refreshStatements(req);

    this.resetPolling(time);
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

  filteredStatementsData = (): AggregateStatistics[] => {
    const { filters } = this.state;
    const { search, statements, nodeRegions, isTenant } = this.props;
    const timeValue = getTimeValueInSeconds(filters);
    const sqlTypes =
      filters.sqlType.length > 0
        ? filters.sqlType.split(",").map(function (sqlType: string) {
            // Adding "Type" to match the value on the Statement
            // Possible values: TypeDDL, TypeDML, TypeDCL and TypeTCL
            return "Type" + sqlType;
          })
        : [];
    const databases =
      filters.database.length > 0 ? filters.database.split(",") : [];
    if (databases.includes(unset)) {
      databases.push("");
    }
    const regions =
      filters.regions.length > 0 ? filters.regions.split(",") : [];
    const nodes = filters.nodes.length > 0 ? filters.nodes.split(",") : [];

    // Return statements filtered by the values selected on the filter and
    // the search text. A statement must match all selected filters to be
    // displayed on the table.
    // Current filters: search text, database, fullScan, service latency,
    // SQL Type, nodes and regions.
    return statements
      .filter(
        statement =>
          databases.length == 0 || databases.includes(statement.database),
      )
      .filter(statement => (filters.fullScan ? statement.fullScan : true))
      .filter(
        statement =>
          statement.stats.service_lat.mean >= timeValue ||
          timeValue === "empty",
      )
      .filter(
        statement =>
          sqlTypes.length == 0 || sqlTypes.includes(statement.stats.sql_type),
      )
      .filter(
        // The statement must contain at least one value from the selected regions
        // list if the list is not empty.
        // If the cluster is a tenant cluster we don't care
        // about regions.
        statement =>
          isTenant ||
          regions.length == 0 ||
          (statement.stats.nodes &&
            containAny(
              statement.stats.nodes.map(
                node => nodeRegions[node.toString()],
                regions,
              ),
              regions,
            )),
      )
      .filter(
        // The statement must contain at least one value from the selected nodes
        // list if the list is not empty.
        // If the cluster is a tenant cluster we don't care
        // about nodes.
        statement =>
          isTenant ||
          nodes.length == 0 ||
          (statement.stats.nodes &&
            containAny(
              statement.stats.nodes.map(node => "n" + node),
              nodes,
            )),
      )
      .filter(statement =>
        search ? filterBySearchQuery(statement, search) : true,
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
    } = this.props;
    const data = this.filteredStatementsData();
    const totalWorkload = calculateTotalWorkload(statements);
    const totalCount = data.length;
    const isEmptySearchResults = statements?.length > 0 && search?.length > 0;
    // If the cluster is a tenant cluster we don't show info
    // about nodes/regions.
    populateRegionNodeForStatements(statements, nodeRegions, isTenant);

    // Creates a list of all possible columns,
    // hiding nodeRegions if is not multi-region and
    // hiding columns that won't be displayed for tenants.
    const columns = makeStatementsColumns(
      statements,
      filters.app.split(","),
      totalWorkload,
      nodeRegions,
      "statement",
      isTenant,
      hasViewActivityRedactedRole,
      search,
      this.activateDiagnosticsRef,
      onSelectDiagnosticsReportDropdownOption,
      onStatementClick,
    )
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

    const nodes = isTenant
      ? []
      : Object.keys(nodeRegions)
          .map(n => Number(n))
          .sort();
    const regions = isTenant
      ? []
      : unique(nodes.map(node => nodeRegions[node.toString()])).sort();
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
              showNodes={nodes.length > 1}
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
