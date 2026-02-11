// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import isNil from "lodash/isNil";
import merge from "lodash/merge";
import moment from "moment-timezone";
import React, {
  useState,
  useEffect,
  useCallback,
  useMemo,
  useRef,
} from "react";
import { RouteComponentProps } from "react-router-dom";

import { SessionsRequest } from "src/api/sessionsApi";
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

// Default filters for sessions page - Active and Idle selected by default
export const defaultFiltersForSessionsPage: Filters = {
  ...defaultFilters,
  sessionStatus: "Active,Idle",
};

// Helper function to determine if closed sessions should be excluded based on filter
export const shouldExcludeClosedSessions = (filters: Filters): boolean => {
  // If no session status filter is set or it's empty (user cleared filters),
  // show all sessions including closed (don't exclude)
  if (!filters?.sessionStatus || filters.sessionStatus === "") {
    return false;
  }
  // If "Closed" is explicitly selected in the filter, include closed sessions
  // Otherwise, exclude closed sessions (e.g., when only "Active,Idle" is selected)
  const selectedStatuses = filters.sessionStatus.split(",");
  return !selectedStatuses.includes("Closed");
};

export interface OwnProps {
  sessions: SessionInfo[];
  internalAppNamePrefix: string;
  sessionsError: Error | Error[];
  sortSetting: SortSetting;
  refreshSessions: (req?: SessionsRequest) => void;
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

export function SessionsPage(props: SessionsPageProps): React.ReactElement {
  const {
    history,
    sessions,
    internalAppNamePrefix,
    sessionsError,
    sortSetting,
    refreshSessions,
    cancelSession,
    cancelQuery,
    onPageChanged,
    onSortingChange,
    onSessionClick,
    onTerminateSessionClick,
    onTerminateStatementClick,
    onColumnsChange,
    onFilterChange,
    columns: userSelectedColumnsToShow,
    filters: propsFilters,
  } = props;

  const terminateSessionRef = useRef<TerminateSessionModalRef>(null);
  const terminateQueryRef = useRef<TerminateQueryModalRef>(null);
  const refreshDataIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Refs to hold latest values for mount effects, avoiding stale closures
  // while preserving "run once on mount" semantics.
  const onSortingChangeRef = useRef(onSortingChange);
  const sortSettingRef = useRef(sortSetting);
  const onFilterChangeRef = useRef(onFilterChange);
  const refreshSessionsRef = useRef(refreshSessions);
  const propsFiltersRef = useRef(propsFilters);
  const sessionsRef = useRef(sessions);

  // Keep refs up to date on each render
  onSortingChangeRef.current = onSortingChange;
  sortSettingRef.current = sortSetting;
  onFilterChangeRef.current = onFilterChange;
  refreshSessionsRef.current = refreshSessions;
  propsFiltersRef.current = propsFilters;
  sessionsRef.current = sessions;

  // Initialize state from history
  const getStateFromHistory = useCallback((): Partial<SessionsPageState> => {
    // Filters.
    const latestFilter = handleFiltersFromQueryString(
      history,
      propsFilters,
      onFilterChange,
    );

    return {
      filters: latestFilter,
    };
  }, [history, propsFilters, onFilterChange]);

  const getInitialState = (): SessionsPageState => {
    const defaultState: SessionsPageState = {
      filters: defaultFiltersForSessionsPage,
      apps: [],
      pagination: {
        pageSize: 20,
        current: 1,
      },
    };

    const stateFromHistory = getStateFromHistory();
    const mergedState = merge({}, defaultState, stateFromHistory);

    // If sessionStatus is not set or empty, use the sessions page default (Active,Idle)
    if (!mergedState.filters?.sessionStatus) {
      mergedState.filters = {
        ...mergedState.filters,
        sessionStatus: defaultFiltersForSessionsPage.sessionStatus,
      };
    }

    return mergedState;
  };

  const [filters, setFilters] = useState<Filters>(
    () => getInitialState().filters,
  );
  const [pagination, setPagination] = useState<ISortedTablePagination>(
    () => getInitialState().pagination,
  );

  // Ref to track current filters state for the refresh interval
  const filtersRef = useRef(filters);
  filtersRef.current = filters;

  // Handle initial sort setting from URL
  useEffect(() => {
    const searchParams = new URLSearchParams(history.location.search);
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;

    if (
      onSortingChangeRef.current &&
      columnTitle &&
      (sortSettingRef.current.columnTitle !== columnTitle ||
        sortSettingRef.current.ascending !== ascending)
    ) {
      onSortingChangeRef.current("Sessions", columnTitle, ascending);
    }
  }, [history.location.search]);

  const resetPagination = useCallback((): void => {
    setPagination(prev => ({
      current: 1,
      pageSize: prev.pageSize,
    }));
  }, []);

  const changeSortSetting = useCallback(
    (ss: SortSetting): void => {
      if (onSortingChange) {
        onSortingChange("Sessions", ss.columnTitle, ss.ascending);
      }

      syncHistory(
        {
          ascending: ss.ascending.toString(),
          columnTitle: ss.columnTitle,
        },
        history,
      );
    },
    [onSortingChange, history],
  );

  const onChangePage = useCallback(
    (current: number, pageSize: number): void => {
      setPagination(prev => ({ ...prev, current, pageSize }));
      if (onPageChanged) {
        onPageChanged(current);
      }
    },
    [onPageChanged],
  );

  const onSubmitFilters = useCallback(
    (newFilters: Filters): void => {
      if (onFilterChange) {
        onFilterChange(newFilters);
      }

      setFilters(newFilters);

      // Refresh sessions with the new filter settings
      const req: SessionsRequest = {
        excludeClosedSessions: shouldExcludeClosedSessions(newFilters),
      };
      refreshSessions(req);

      resetPagination();
      syncHistory(
        {
          app: newFilters.app,
          timeNumber: newFilters.timeNumber,
          timeUnit: newFilters.timeUnit,
          sessionStatus: newFilters.sessionStatus,
        },
        history,
      );
    },
    [onFilterChange, refreshSessions, resetPagination, history],
  );

  const onClearFilters = useCallback((): void => {
    if (onFilterChange) {
      onFilterChange(defaultFilters);
    }

    setFilters({ ...defaultFilters });

    // Refresh sessions with cleared filters (include all sessions)
    const req: SessionsRequest = {
      excludeClosedSessions: shouldExcludeClosedSessions(defaultFilters),
    };
    refreshSessions(req);

    resetPagination();
    syncHistory(
      {
        app: undefined,
        timeNumber: undefined,
        timeUnit: undefined,
        sessionStatus: undefined,
      },
      history,
    );
  }, [onFilterChange, refreshSessions, resetPagination, history]);

  const filteredSessionsData = useMemo((): {
    sessions: SessionInfo[];
    activeFilters: number;
  } => {
    if (!sessions) {
      return {
        sessions: [],
        activeFilters: 0,
      };
    }
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
  }, [filters, sessions, internalAppNamePrefix]);

  // componentDidMount
  useEffect(() => {
    // If no filters are stored in localStorage, set the default filters
    if (!propsFiltersRef.current || !propsFiltersRef.current.sessionStatus) {
      onFilterChangeRef.current?.(defaultFiltersForSessionsPage);
    }

    if (!sessionsRef.current || sessionsRef.current.length === 0) {
      const currentFilters = filtersRef.current || propsFiltersRef.current;
      refreshSessionsRef.current({
        excludeClosedSessions: shouldExcludeClosedSessions(currentFilters),
      });
    }

    refreshDataIntervalRef.current = setInterval(() => {
      const currentFilters = filtersRef.current || propsFiltersRef.current;
      refreshSessionsRef.current({
        excludeClosedSessions: shouldExcludeClosedSessions(currentFilters),
      });
    }, 10 * 1000);

    return () => {
      if (refreshDataIntervalRef.current) {
        clearInterval(refreshDataIntervalRef.current);
      }
    };
  }, []);

  // componentDidUpdate - update sort setting query params
  useEffect(() => {
    updateSortSettingQueryParamsOnTab(
      "Sessions",
      sortSetting,
      {
        ascending: false,
        columnTitle: "statementAge",
      },
      history,
    );
  }, [sortSetting, history]);

  const renderSessions = useCallback((): React.ReactElement => {
    const sessionsData = sessions;

    const { sessions: sessionsToDisplay, activeFilters } = filteredSessionsData;

    const appNames = getSessionAppFilterOptions(sessionsData);
    const usernames = getSessionUsernameFilterOptions(sessionsData);
    const sessionStatuses = sessionStatusFilterOptions;
    const columns = makeSessionsColumns(
      "session",
      terminateSessionRef,
      terminateQueryRef,
      onSessionClick,
      onTerminateStatementClick,
      onTerminateSessionClick,
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
            onSubmitFilters={onSubmitFilters}
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
            onRemoveFilter={onSubmitFilters}
            onClearFilters={onClearFilters}
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
            sortSetting={sortSetting}
            onChangeSortSetting={changeSortSetting}
            pagination={pagination}
          />
        </section>
        <Pagination
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={sessionsToDisplay.length}
          onChange={onChangePage}
          onShowSizeChange={onChangePage}
        />
      </>
    );
  }, [
    sessions,
    filteredSessionsData,
    onSessionClick,
    onTerminateStatementClick,
    onTerminateSessionClick,
    userSelectedColumnsToShow,
    onSubmitFilters,
    filters,
    onClearFilters,
    onColumnsChange,
    pagination,
    sortSetting,
    changeSortSetting,
    onChangePage,
  ]);

  return (
    <div className={sessionsPageCx("sessions-page")}>
      <Loading
        loading={isNil(sessions)}
        page={"sessions"}
        error={sessionsError}
        render={renderSessions}
        renderError={() =>
          LoadingError({
            statsType: "sessions",
            error: mergeErrors(sessionsError),
          })
        }
      />
      <TerminateSessionModal ref={terminateSessionRef} cancel={cancelSession} />
      <TerminateQueryModal ref={terminateQueryRef} cancel={cancelQuery} />
    </div>
  );
}

export default SessionsPage;
