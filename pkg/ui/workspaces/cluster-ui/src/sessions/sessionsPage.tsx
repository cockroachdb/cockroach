// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import isNil from "lodash/isNil";
import merge from "lodash/merge";
import moment from "moment-timezone";
import React from "react";
import { RouteComponentProps } from "react-router-dom";

import { Loading } from "src/loading";
import { Pagination } from "src/pagination";
import {
  SortSetting,
  ISortedTablePagination,
  updateSortSettingQueryParamsOnTab,
  ColumnDescriptor,
} from "src/sortedtable";
import statementsPageStyles from "src/statementsPage/statementsPage.module.scss";
import {
  ICancelSessionRequest,
  ICancelQueryRequest,
} from "src/store/terminateQuery";
import { TimestampToMoment, unset } from "src/util";
import { syncHistory } from "src/util/query";

import ColumnsSelector, {
  SelectOption,
} from "../columnsSelector/columnsSelector";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  Filters,
  getTimeValueInSeconds,
  handleFiltersFromQueryString,
  SelectedFilters,
} from "../queryFilter";
import LoadingError, { mergeErrors } from "../sqlActivity/errorComponent";
import {
  getLabel,
  StatisticTableColumnKeys,
} from "../statsTableUtil/statsTableUtil";
import { TableStatistics } from "../tableStatistics";

import { EmptySessionsTablePlaceholder } from "./emptySessionsTablePlaceholder";
import sessionPageStyles from "./sessionPage.module.scss";
import {
  getStatusString,
  makeSessionsColumns,
  SessionInfo,
  SessionsSortedTable,
} from "./sessionsTable";
import TerminateQueryModal, {
  TerminateQueryModalRef,
} from "./terminateQueryModal";
import TerminateSessionModal, {
  TerminateSessionModalRef,
} from "./terminateSessionModal";

const statementsPageCx = classNames.bind(statementsPageStyles);
const sessionsPageCx = classNames.bind(sessionPageStyles);

const sessionStatusFilterOptions = ["Active", "Closed", "Idle"];

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
    sessions.map(s => {
      if (s.session.application_name.startsWith("$ internal")) {
        return "$ internal";
      }
      return s.session.application_name ? s.session.application_name : unset;
    }),
  );

  return Array.from(uniqueAppNames).sort();
}

function getSessionUsernameFilterOptions(sessions: SessionInfo[]): string[] {
  const uniqueUsernames = new Set(sessions.map(s => s.session.username));

  return Array.from(uniqueUsernames).sort();
}

export class SessionsPage extends React.Component<
  SessionsPageProps,
  SessionsPageState
> {
  terminateSessionRef: React.RefObject<TerminateSessionModalRef>;
  terminateQueryRef: React.RefObject<TerminateQueryModalRef>;
  refreshDataInterval: NodeJS.Timeout;

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
      (sortSetting.columnTitle !== columnTitle ||
        sortSetting.ascending !== ascending)
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
    if (!this.props.sessions || this.props.sessions.length === 0) {
      this.props.refreshSessions();
    }

    this.refreshDataInterval = setInterval(
      this.props.refreshSessions,
      10 * 1000,
    );
  }

  componentWillUnmount(): void {
    if (!this.refreshDataInterval) return;
    clearInterval(this.refreshDataInterval);
  }

  componentDidUpdate = (): void => {
    const { history, sortSetting } = this.props;

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
    if (this.props.onPageChanged) {
      this.props.onPageChanged(current);
    }
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
        if (filters.app && filters.app !== "All") {
          const apps = filters.app.split(",");
          let showInternal = false;
          if (apps.includes(internalAppNamePrefix)) {
            showInternal = true;
          }
          if (apps.includes(unset)) {
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
        return timeValue === "empty" || sessionTime >= Number(timeValue);
      })
      .filter((s: SessionInfo) => {
        if (filters.username && filters.username !== "All") {
          const usernames = filters.username.split(",");
          return usernames.includes(s.session.username);
        }
        return true;
      })
      .filter((s: SessionInfo) => {
        if (filters.sessionStatus && filters.sessionStatus !== "All") {
          const statuses = filters.sessionStatus.split(",");
          return statuses.includes(getStatusString(s.session.status));
        }
        return true;
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

    const { sessions: sessionsToDisplay, activeFilters } =
      this.getFilteredSessionsData();

    const appNames = getSessionAppFilterOptions(sessionsData);
    const usernames = getSessionUsernameFilterOptions(sessionsData);
    const sessionStatuses = sessionStatusFilterOptions;
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
        (userSelectedColumnsToShow === null && c.showByDefault !== false) ||
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

    const displayColumns = columns.filter(c => isColumnSelected(c));

    return (
      <>
        <div className={sessionsPageCx("sessions-filter")}>
          <Filter
            onSubmitFilters={this.onSubmitFilters}
            appNames={appNames}
            usernames={usernames}
            showUsername={true}
            showSessionStatus={true}
            sessionStatuses={sessionStatuses}
            activeFilters={activeFilters}
            filters={filters}
            timeLabel={"Session duration"}
          />
          <SelectedFilters
            filters={filters}
            onRemoveFilter={this.onSubmitFilters}
            onClearFilters={this.onClearFilters}
          />
        </div>
        <section className={sessionsPageCx("sessions-table-area")}>
          <div className={statementsPageCx("cl-table-statistic")}>
            <div className={"session-column-selector"}>
              <ColumnsSelector
                options={tableColumns}
                onSubmitColumns={onColumnsChange}
                size={"small"}
              />
              <TableStatistics
                pagination={pagination}
                totalCount={sessionsToDisplay.length}
                arrayItemName="sessions"
                activeFilters={activeFilters}
              />
            </div>
          </div>
          <SessionsSortedTable
            className="sessions-table"
            data={sessionsToDisplay}
            columns={displayColumns}
            renderNoResult={
              <EmptySessionsTablePlaceholder
                isEmptySearchResults={
                  activeFilters > 0 && sessionsToDisplay.length === 0
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
          total={sessionsToDisplay.length}
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
            LoadingError({
              statsType: "sessions",
              error: mergeErrors(this.props.sessionsError),
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
