// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { isNil, merge } from "lodash";

import { syncHistory } from "src/util/query";
import {
  makeSessionsColumns,
  SessionInfo,
  SessionsSortedTable,
} from "./sessionsTable";
import { RouteComponentProps } from "react-router-dom";
import classNames from "classnames/bind";
import { sessionsTable } from "src/util/docs";

import emptyTableResultsIcon from "../assets/emptyState/empty-table-results.svg";
import SQLActivityError from "../sqlActivity/errorComponent";
import { Pagination } from "src/pagination";
import {
  SortSetting,
  ISortedTablePagination,
  updateSortSettingQueryParamsOnTab,
  ColumnDescriptor,
} from "src/sortedtable";
import { Loading } from "src/loading";
import { Anchor } from "src/anchor";
import { EmptyTable } from "src/empty";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  Filters,
  getTimeValueInSeconds,
  handleFiltersFromQueryString,
} from "../queryFilter";

import TerminateQueryModal, {
  TerminateQueryModalRef,
} from "./terminateQueryModal";
import TerminateSessionModal, {
  TerminateSessionModalRef,
} from "./terminateSessionModal";

import {
  ICancelSessionRequest,
  ICancelQueryRequest,
} from "src/store/terminateQuery";

import statementsPageStyles from "src/statementsPage/statementsPage.module.scss";
import sessionPageStyles from "./sessionPage.module.scss";
import ColumnsSelector, {
  SelectOption,
} from "../columnsSelector/columnsSelector";
import { TimestampToMoment } from "src/util";
import moment from "moment";
import {
  getLabel,
  StatisticTableColumnKeys,
} from "../statsTableUtil/statsTableUtil";
import { TableStatistics } from "../tableStatistics";
import * as protos from "@cockroachlabs/crdb-protobuf-client";

type ISessionsResponse = protos.cockroach.server.serverpb.IListSessionsResponse;

const statementsPageCx = classNames.bind(statementsPageStyles);
const sessionsPageCx = classNames.bind(sessionPageStyles);

export interface OwnProps {
  sessions: SessionInfo[];
  internalAppNamePrefix: string;
  sessionsError: Error | Error[];
  sortSetting: SortSetting;
  refreshSessions: () => void;
  cancelSession: (payload: ICancelSessionRequest) => void;
  cancelQuery: (payload: ICancelQueryRequest) => void;
  onPageChanged?: (newPage: number) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onSessionClick?: () => void;
  onTerminateSessionClick?: () => void;
  onTerminateStatementClick?: () => void;
  onColumnsChange?: (selectedColumns: string[]) => void;
  onFilterChange?: (value: Filters) => void;
  columns: string[];
  filters: Filters;
}

export interface SessionsPageState {
  apps: string[];
  pagination: ISortedTablePagination;
  filters: Filters;
  activeFilters?: number;
}

export type SessionsPageProps = OwnProps & RouteComponentProps;

function getSessionAppFilterOptions(sessions: SessionInfo[]): string[] {
  const uniqueAppNames = new Set(
    sessions.map(s =>
      s.session.application_name ? s.session.application_name : "(unset)",
    ),
  );

  return Array.from(uniqueAppNames);
}

export class SessionsPage extends React.Component<
  SessionsPageProps,
  SessionsPageState
> {
  terminateSessionRef: React.RefObject<TerminateSessionModalRef>;
  terminateQueryRef: React.RefObject<TerminateQueryModalRef>;

  constructor(props: SessionsPageProps) {
    super(props);
    this.state = {
      filters: defaultFilters,
      apps: [],
      pagination: {
        pageSize: 20,
        current: 1,
      },
    };

    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(this.state, stateFromHistory);
    this.terminateSessionRef = React.createRef();
    this.terminateQueryRef = React.createRef();

    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;
    const sortSetting = this.props.sortSetting;

    if (
      this.props.onSortingChange &&
      columnTitle &&
      (sortSetting.columnTitle != columnTitle ||
        sortSetting.ascending != ascending)
    ) {
      this.props.onSortingChange("Sessions", columnTitle, ascending);
    }
  }

  getStateFromHistory = (): Partial<SessionsPageState> => {
    const { history, filters, onFilterChange } = this.props;

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
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Sessions", ss.columnTitle, ss.ascending);
    }

    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      this.props.history,
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

  componentDidMount(): void {
    this.props.refreshSessions();
  }

  componentDidUpdate = (): void => {
    const { history, sortSetting } = this.props;

    this.props.refreshSessions();
    updateSortSettingQueryParamsOnTab(
      "Sessions",
      sortSetting,
      {
        ascending: false,
        columnTitle: "statementAge",
      },
      history,
    );
  };

  onChangePage = (current: number): void => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
    this.props.onPageChanged(current);
  };

  onSubmitFilters = (filters: Filters): void => {
    if (this.props.onFilterChange) {
      this.props.onFilterChange(filters);
    }

    this.setState({
      filters: filters,
    });
    this.resetPagination();
    syncHistory(
      {
        app: filters.app,
        timeNumber: filters.timeNumber,
        timeUnit: filters.timeUnit,
      },
      this.props.history,
    );
  };

  onClearFilters = (): void => {
    if (this.props.onFilterChange) {
      this.props.onFilterChange(defaultFilters);
    }

    this.setState({
      filters: {
        ...defaultFilters,
      },
    });
    this.resetPagination();
    syncHistory(
      {
        app: undefined,
        timeNumber: undefined,
        timeUnit: undefined,
      },
      this.props.history,
    );
  };

  getFilteredSessionsData = (): {
    sessions: SessionInfo[];
    activeFilters: number;
  } => {
    const { filters } = this.state;
    const { sessions, internalAppNamePrefix } = this.props;
    if (!filters) {
      return {
        sessions: sessions,
        activeFilters: 0,
      };
    }
    const activeFilters = calculateActiveFilters(filters);
    const timeValue = getTimeValueInSeconds(filters);
    const filteredSessions = sessions
      .filter((s: SessionInfo) => {
        const isInternal = (s: SessionInfo) =>
          s.session.application_name.startsWith(internalAppNamePrefix);
        if (filters.app && filters.app != "All") {
          const apps = filters.app.split(",");
          let showInternal = false;
          if (apps.includes(internalAppNamePrefix)) {
            showInternal = true;
          }
          if (apps.includes("(unset)")) {
            apps.push("");
          }

          return (
            (showInternal && isInternal(s)) ||
            apps.includes(s.session.application_name)
          );
        } else {
          return !isInternal(s);
        }
      })
      .filter((s: SessionInfo) => {
        const sessionTime = moment().diff(
          TimestampToMoment(s.session.start),
          "seconds",
        );
        return sessionTime >= timeValue || timeValue === "empty";
      });

    return {
      sessions: filteredSessions,
      activeFilters,
    };
  };

  renderSessions = (): React.ReactElement => {
    const sessionsData = this.props.sessions;
    const { pagination, filters } = this.state;
    const { columns: userSelectedColumnsToShow, onColumnsChange } = this.props;

    const {
      sessions: sessionsToDisplay,
      activeFilters,
    } = this.getFilteredSessionsData();

    const appNames = getSessionAppFilterOptions(sessionsData);
    const columns = makeSessionsColumns(
      "session",
      this.terminateSessionRef,
      this.terminateQueryRef,
      this.props.onSessionClick,
      this.props.onTerminateStatementClick,
      this.props.onTerminateSessionClick,
    );

    const isColumnSelected = (c: ColumnDescriptor<SessionInfo>) => {
      return (
        (!userSelectedColumnsToShow && c.showByDefault) ||
        (userSelectedColumnsToShow &&
          userSelectedColumnsToShow.includes(c.name)) ||
        c.alwaysShow
      );
    };

    const tableColumns = columns
      .filter(c => !c.alwaysShow)
      .map(
        (c): SelectOption => ({
          label: getLabel(c.name as StatisticTableColumnKeys),
          value: c.name,
          isSelected: isColumnSelected(c),
        }),
      );

    const timeLabel = "Session duration runs longer than";
    const displayColumns = columns.filter(c => isColumnSelected(c));

    return (
      <>
        <div className={sessionsPageCx("sessions-filter")}>
          <Filter
            onSubmitFilters={this.onSubmitFilters}
            appNames={appNames}
            activeFilters={activeFilters}
            filters={filters}
            timeLabel={timeLabel}
          />
        </div>
        <section>
          <div className={statementsPageCx("cl-table-statistic")}>
            <div className={"session-column-selector"}>
              <ColumnsSelector
                options={tableColumns}
                onSubmitColumns={onColumnsChange}
              />
              <TableStatistics
                pagination={pagination}
                totalCount={sessionsToDisplay.length}
                arrayItemName="sessions"
                activeFilters={activeFilters}
                onClearFilters={this.onClearFilters}
              />
            </div>
          </div>
          <SessionsSortedTable
            className="sessions-table"
            data={sessionsToDisplay}
            columns={displayColumns}
            renderNoResult={
              <EmptyTable
                title="No sessions are currently running"
                icon={emptyTableResultsIcon}
                message="Sessions show you which statements and transactions are running for the active session."
                footer={
                  <Anchor href={sessionsTable} target="_blank">
                    Learn more about sessions
                  </Anchor>
                }
              />
            }
            sortSetting={this.props.sortSetting}
            onChangeSortSetting={this.changeSortSetting}
            pagination={pagination}
          />
        </section>
        <Pagination
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={sessionsData.length}
          onChange={this.onChangePage}
        />
      </>
    );
  };

  render(): React.ReactElement {
    const { cancelSession, cancelQuery } = this.props;
    return (
      <div className={sessionsPageCx("sessions-page")}>
        <Loading
          loading={isNil(this.props.sessions)}
          page={"sessions"}
          error={this.props.sessionsError}
          render={this.renderSessions}
          renderError={() =>
            SQLActivityError({
              statsType: "sessions",
            })
          }
        />
        <TerminateSessionModal
          ref={this.terminateSessionRef}
          cancel={cancelSession}
        />
        <TerminateQueryModal
          ref={this.terminateQueryRef}
          cancel={cancelQuery}
        />
      </div>
    );
  }
}

export default SessionsPage;
