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
import { Loading } from "src/loading";
import {
  handleSortSettingFromQueryString,
  SortSetting,
  updateSortSettingQueryParamsOnTab,
} from "src/sortedtable";
import {
  Filters,
  defaultFilters,
  handleFiltersFromQueryString,
  updateFiltersQueryParamsOnTab,
} from "../queryFilter";

import { unique, syncHistory } from "src/util";
import { AggregateStatistics } from "../statementsTable";
import {
  ActivateStatementDiagnosticsModal,
  ActivateDiagnosticsModalRef,
} from "src/statementsDiagnostics";
import { ISortedTablePagination } from "../sortedtable";
import styles from "./statementsPage.module.scss";
import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";

type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
type IDuration = google.protobuf.IDuration;
import { UIConfigState } from "../store";
import { StatementsRequest } from "src/api/statementsApi";
import Long from "long";
import SQLActivityError from "../sqlActivity/errorComponent";
import { TimeScale, toDateRange } from "../timeScaleDropdown";

import SQLStatsPageSettings from "./statsPageSettings";
import StatementsTableWrapper from "./statementsTableWrapper";
const cx = classNames.bind(styles);

// Most of the props are supposed to be provided as connected props
// from redux store.
// StatementsPageDispatchProps, StatementsPageStateProps, and StatementsPageOuterProps interfaces
// provide convenient definitions for `mapDispatchToProps`, `mapStateToProps` and props that
// have to be provided by parent component.
export interface StatementsPageDispatchProps {
  refreshStatements: (req?: StatementsRequest) => void;
  refreshStatementDiagnosticsRequests: () => void;
  refreshUserSQLRoles: () => void;
  resetSQLStats: () => void;
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
}

export interface StatementsPageState {
  pagination: ISortedTablePagination;
  filters?: Filters;
}

export type StatementsPageProps = StatementsPageDispatchProps &
  StatementsPageStateProps &
  RouteComponentProps<unknown>;

function statementsRequestFromProps(
  props: StatementsPageProps,
): cockroach.server.serverpb.StatementsRequest {
  const [start, end] = toDateRange(props.timeScale);
  return new cockroach.server.serverpb.StatementsRequest({
    combined: true,
    start: Long.fromNumber(start.unix()),
    end: Long.fromNumber(end.unix()),
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

  static defaultProps: Partial<StatementsPageProps> = {
    isTenant: false,
    hasViewActivityRedactedRole: false,
  };

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
  };

  refreshStatements = (): void => {
    const req = statementsRequestFromProps(this.props);
    this.props.refreshStatements(req);
  };

  componentDidMount(): void {
    this.refreshStatements();
    this.props.refreshUserSQLRoles();
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
    if (!this.props.isTenant && !this.props.hasViewActivityRedactedRole) {
      this.props.refreshStatementDiagnosticsRequests();
    }
  };

  componentWillUnmount(): void {
    this.props.dismissAlertMessage();
  }

  onSubmitFilters = (filters: Filters): void => {
    if (this.props.onFilterChange) {
      this.props.onFilterChange(filters);
    }

    this.setState({
      filters: filters,
    });

    // Set all filters to undefined for history.
    const stringifiedFilters: Record<string, string> = {};

    Object.entries(filters).forEach(([filter, val]) => {
      stringifiedFilters[filter] =
        val === defaultFilters[filter as keyof Filters]
          ? undefined
          : val.toString();
    });

    syncHistory(stringifiedFilters, this.props.history);
  };

  onClearFilters = (): void => {
    this.onSubmitFilters(defaultFilters);
  };

  render(): React.ReactElement {
    const {
      apps,
      columns,
      databases,
      hasViewActivityRedactedRole,
      isTenant,
      nodeRegions,
      search,
      sortSetting,
      statements,
      timeScale,
      onActivateStatementDiagnostics,
      onColumnsChange,
      onDiagnosticsModalOpen,
      onPageChanged,
      onSearchComplete,
      onSelectDiagnosticsReportDropdownOption,
      onSortingChange,
      onStatementClick,
      refreshStatementDiagnosticsRequests,
      resetSQLStats,
    } = this.props;

    const nodes = isTenant
      ? []
      : Object.keys(nodeRegions)
          .map(n => Number(n))
          .sort();
    const regions = isTenant
      ? []
      : unique(nodes.map(node => nodeRegions[node.toString()])).sort();
    const { filters } = this.state;

    return (
      <div className={cx("root", "table-area")}>
        <SQLStatsPageSettings
          apps={apps}
          databases={databases}
          filters={filters}
          nodes={nodes}
          statType="statement"
          regions={regions}
          searchInit={search}
          timeScale={timeScale}
          onChangeTimeScale={this.changeTimeScale}
          onSearchSubmit={onSearchComplete}
          onSubmitFilters={this.onSubmitFilters}
          resetSQLStats={resetSQLStats}
        />
        <Loading
          loading={isNil(this.props.statements)}
          page={"statements"}
          error={this.props.statementsError}
          render={
            <StatementsTableWrapper
              activateDiagnosticsRef={this.activateDiagnosticsRef}
              columns={columns}
              filters={filters}
              hasViewActivityRedactedRole={hasViewActivityRedactedRole}
              isTenant={isTenant}
              nodeRegions={nodeRegions}
              search={search}
              sortSetting={sortSetting}
              statements={statements}
              onClearFilters={this.onClearFilters}
              onColumnsChange={onColumnsChange}
              onPageChanged={onPageChanged}
              onSelectDiagnosticsReportDropdownOption={
                onSelectDiagnosticsReportDropdownOption
              }
              onSortingChange={onSortingChange}
              onStatementClick={onStatementClick}
            />
          }
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
