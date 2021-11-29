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
import moment, { Moment } from "moment";
import classNames from "classnames/bind";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import {
  ColumnDescriptor,
  handleSortSettingFromQueryString,
  SortSetting,
  updateSortSettingQueryParamsOnTab,
} from "src/sortedtable";
import { Search } from "src/search";
import { Pagination } from "src/pagination";
import { DateRange } from "src/dateRange";
import { TableStatistics } from "../tableStatistics";
import {
  Filter,
  Filters,
  defaultFilters,
  calculateActiveFilters,
  getTimeValueInSeconds,
  handleFiltersFromQueryString,
  updateFiltersQueryParamsOnTab,
} from "../queryFilter";

import {
  calculateTotalWorkload,
  unique,
  containAny,
  syncHistory,
} from "src/util";
import {
  AggregateStatistics,
  populateRegionNodeForStatements,
  makeStatementsColumns,
  StatementsSortedTable,
} from "../statementsTable";
import {
  getLabel,
  StatisticTableColumnKeys,
} from "../statsTableUtil/statsTableUtil";
import {
  ActivateStatementDiagnosticsModal,
  ActivateDiagnosticsModalRef,
} from "src/statementsDiagnostics";
import { ISortedTablePagination } from "../sortedtable";
import styles from "./statementsPage.module.scss";
import { EmptyStatementsPlaceholder } from "./emptyStatementsPlaceholder";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import ColumnsSelector from "../columnsSelector/columnsSelector";
import { SelectOption } from "../multiSelectCheckbox/multiSelectCheckbox";
import { UIConfigState } from "../store";
import { StatementsRequest } from "src/api/statementsApi";
import Long from "long";
import ClearStats from "../sqlActivity/clearStats";
import SQLActivityError from "../sqlActivity/errorComponent";
import { commonStyles } from "../common";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

// Most of the props are supposed to be provided as connected props
// from redux store.
// StatementsPageDispatchProps, StatementsPageStateProps, and StatementsPageOuterProps interfaces
// provide convenient definitions for `mapDispatchToProps`, `mapStateToProps` and props that
// have to be provided by parent component.
export interface StatementsPageDispatchProps {
  refreshStatements: (req?: StatementsRequest) => void;
  refreshStatementDiagnosticsRequests: () => void;
  resetSQLStats: () => void;
  dismissAlertMessage: () => void;
  onActivateStatementDiagnostics: (statement: string) => void;
  onDiagnosticsModalOpen?: (statement: string) => void;
  onSearchComplete?: (query: string) => void;
  onPageChanged?: (newPage: number) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onDiagnosticsReportDownload?: (report: IStatementDiagnosticsReport) => void;
  onFilterChange?: (value: Filters) => void;
  onStatementClick?: (statement: string) => void;
  onColumnsChange?: (selectedColumns: string[]) => void;
  onDateRangeChange: (start: Moment, end: Moment) => void;
}

export interface StatementsPageStateProps {
  statements: AggregateStatistics[];
  dateRange: [Moment, Moment];
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
}

export interface StatementsPageState {
  pagination: ISortedTablePagination;
  filters?: Filters;
  activeFilters?: number;
}

export type StatementsPageProps = StatementsPageDispatchProps &
  StatementsPageStateProps &
  RouteComponentProps<unknown>;

function statementsRequestFromProps(
  props: StatementsPageProps,
): cockroach.server.serverpb.StatementsRequest {
  return new cockroach.server.serverpb.StatementsRequest({
    combined: true,
    start: Long.fromNumber(props.dateRange[0].unix()),
    end: Long.fromNumber(props.dateRange[1].unix()),
  });
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
        pageSize: 20,
        current: 1,
      },
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

  changeDateRange = (start: Moment, end: Moment): void => {
    if (this.props.onDateRangeChange) {
      this.props.onDateRangeChange(start, end);
    }
  };

  resetTime = (): void => {
    // Default range to reset to is one hour ago.
    this.changeDateRange(
      moment.utc().subtract(1, "hours"),
      moment.utc().add(1, "minute"),
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
    const req = statementsRequestFromProps(this.props);
    this.props.refreshStatements(req);
  };

  componentDidMount(): void {
    this.refreshStatements();
    if (!this.props.isTenant) {
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
    this.refreshStatements();
    if (!this.props.isTenant) {
      this.props.refreshStatementDiagnosticsRequests();
    }
  };

  componentWillUnmount(): void {
    this.props.dismissAlertMessage();
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
        ? filters.sqlType.split(",").map(function(sqlType: string) {
            // Adding "Type" to match the value on the Statement
            // Possible values: TypeDDL, TypeDML, TypeDCL and TypeTCL
            return "Type" + sqlType;
          })
        : [];
    const databases =
      filters.database.length > 0 ? filters.database.split(",") : [];
    if (databases.includes("(unset)")) {
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
      .filter(statement =>
        search
          ? search
              .split(" ")
              .every(val =>
                statement.label.toLowerCase().includes(val.toLowerCase()),
              )
          : true,
      )
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
      );
  };

  renderStatements = (): React.ReactElement => {
    const { pagination, filters, activeFilters } = this.state;
    const {
      statements,
      apps,
      databases,
      onDiagnosticsReportDownload,
      onStatementClick,
      resetSQLStats,
      columns: userSelectedColumnsToShow,
      onColumnsChange,
      nodeRegions,
      isTenant,
      sortSetting,
      search,
    } = this.props;
    const data = this.filteredStatementsData();
    const totalWorkload = calculateTotalWorkload(data);
    const totalCount = data.length;
    const isEmptySearchResults = statements?.length > 0 && search?.length > 0;
    // If the cluster is a tenant cluster we don't show info
    // about nodes/regions.
    const nodes = isTenant
      ? []
      : Object.keys(nodeRegions)
          .map(n => Number(n))
          .sort();
    const regions = isTenant
      ? []
      : unique(nodes.map(node => nodeRegions[node.toString()])).sort();
    populateRegionNodeForStatements(statements, nodeRegions, isTenant);

    // Creates a list of all possible columns,
    // hiding nodeRegions if is not multi-region and
    // hiding columns that won't be displayed for tenants.
    const columns = makeStatementsColumns(
      statements,
      filters.app,
      totalWorkload,
      nodeRegions,
      "statement",
      isTenant,
      search,
      this.activateDiagnosticsRef,
      onDiagnosticsReportDownload,
      onStatementClick,
    )
      .filter(c => !(c.name === "regionNodes" && regions.length < 2))
      .filter(c => !(isTenant && c.hideIfTenant));

    const isColumnSelected = (c: ColumnDescriptor<AggregateStatistics>) => {
      return (
        (userSelectedColumnsToShow === null && c.showByDefault !== false) || // show column if list of visible was never defined and can be show by default.
        (userSelectedColumnsToShow !== null &&
          userSelectedColumnsToShow.includes(c.name)) || // show column if user changed its visibility.
        c.alwaysShow === true // show column if alwaysShow option is set explicitly.
      );
    };

    // Iterate over all available columns and create list of SelectOptions with initial selection
    // values based on stored user selections in local storage and default column configs.
    // Columns that are set to alwaysShow are filtered from the list.
    const tableColumns = columns
      .filter(c => !c.alwaysShow)
      .map(
        (c): SelectOption => ({
          label: getLabel(c.name as StatisticTableColumnKeys, "statement"),
          value: c.name,
          isSelected: isColumnSelected(c),
        }),
      );

    // List of all columns that will be displayed based on the column selection.
    const displayColumns = columns.filter(c => isColumnSelected(c));

    return (
      <div>
        <PageConfig>
          <PageConfigItem>
            <Search
              onSubmit={this.onSubmitSearchField as any}
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
          <PageConfigItem>
            <DateRange
              start={this.props.dateRange[0]}
              end={this.props.dateRange[1]}
              onSubmit={this.changeDateRange}
            />
          </PageConfigItem>
          <PageConfigItem>
            <button className={cx("reset-btn")} onClick={this.resetTime}>
              reset time
            </button>
          </PageConfigItem>
          <PageConfigItem className={commonStyles("separator")}>
            <ClearStats resetSQLStats={resetSQLStats} tooltipType="statement" />
          </PageConfigItem>
        </PageConfig>
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
    } = this.props;
    return (
      <div className={cx("root", "table-area")}>
        <Loading
          loading={isNil(this.props.statements)}
          error={this.props.statementsError}
          render={this.renderStatements}
          renderError={() =>
            SQLActivityError({
              statsType: "statements",
            })
          }
        />
        <ActivateStatementDiagnosticsModal
          ref={this.activateDiagnosticsRef}
          activate={onActivateStatementDiagnostics}
          refreshDiagnosticsReports={refreshStatementDiagnosticsRequests}
          onOpenModal={onDiagnosticsModalOpen}
        />
      </div>
    );
  }
}
