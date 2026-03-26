// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import { message } from "antd";
import moment from "moment-timezone";
import React, { useEffect, useMemo, useRef, useState } from "react";
import { useHistory } from "react-router-dom";

import { useSessions } from "src/api/sessionsApi";
import {
  terminateSession,
  terminateQuery,
  CancelSessionRequestMessage,
  CancelQueryRequestMessage,
} from "src/api/terminateQueryApi";
import { Loading } from "src/loading";
import { Pagination } from "src/pagination";
import { SortSetting, ColumnDescriptor } from "src/sortedtable";
import statementsPageStyles from "src/statementsPage/statementsPage.module.scss";
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
  getFullFiltersAsStringRecord,
  getTimeValueInSeconds,
  SelectedFilters,
} from "../queryFilter";
import { getFiltersFromURL } from "../queryFilter/utils";
import { getTableSortFromURL } from "../sortedtable/getTableSortFromURL";
import LoadingError from "../sqlActivity/errorComponent";
import {
  getLabel,
  StatisticTableColumnKeys,
} from "../statsTableUtil/statsTableUtil";
import { TableStatistics } from "../tableStatistics";
import { usePagination } from "../util";

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

const PAGE_SIZE = 20;
const sessionStatusFilterOptions = ["Active", "Closed", "Idle"];

const DEFAULT_SORT_SETTING: SortSetting = {
  ascending: false,
  columnTitle: "statementAge",
};

// Default filters for sessions page - Active and Idle selected by default.
export const defaultFiltersForSessionsPage: Filters = {
  ...defaultFilters,
  sessionStatus: "Active,Idle",
};

// Helper function to determine if closed sessions should be excluded
// based on the current filter selection.
export const shouldExcludeClosedSessions = (filters: Filters): boolean => {
  if (!filters?.sessionStatus || filters.sessionStatus === "") {
    return false;
  }
  const selectedStatuses = filters.sessionStatus.split(",");
  return !selectedStatuses.includes("Closed");
};

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

export const SessionsPage: React.FC = () => {
  const history = useHistory();

  const terminateSessionRef = useRef<TerminateSessionModalRef>(null);
  const terminateQueryRef = useRef<TerminateQueryModalRef>(null);

  // Initialize sort and filters from URL params, falling back to defaults.
  const [sortSetting, setSortSetting] = useState<SortSetting>(
    () => getTableSortFromURL(history.location) ?? DEFAULT_SORT_SETTING,
  );
  const [filters, setFilters] = useState<Filters>(
    () =>
      ({
        ...defaultFiltersForSessionsPage,
        ...getFiltersFromURL(history.location),
      }) as Filters,
  );
  const [selectedColumns, setSelectedColumns] = useState<string[] | null>(null);

  const [pagination, updatePagination, resetPagination] = usePagination(
    1,
    PAGE_SIZE,
  );

  // Fetch sessions data via SWR. The excludeClosedSessions param is
  // derived from the current filter state so that changing the filter
  // automatically triggers a fetch with the correct params.
  const {
    data: sessionsResponse,
    isLoading: sessionsLoading,
    error: sessionsError,
  } = useSessions({
    excludeClosedSessions: shouldExcludeClosedSessions(filters),
    refreshInterval: 10_000,
  });

  const sessions: SessionInfo[] | null = useMemo(() => {
    if (!sessionsResponse) return null;
    return sessionsResponse.sessions.map(session => ({ session }));
  }, [sessionsResponse]);

  const internalAppNamePrefix: string | null =
    sessionsResponse?.internal_app_name_prefix ?? null;

  // Sync sort and filters to URL whenever they change.
  useEffect(() => {
    syncHistory(
      {
        ascending: sortSetting.ascending.toString(),
        columnTitle: sortSetting.columnTitle,
        ...getFullFiltersAsStringRecord(filters),
      },
      history,
      true,
    );
  }, [history, filters, sortSetting.ascending, sortSetting.columnTitle]);

  const onSortClick = (ss: SortSetting): void => {
    setSortSetting(ss);
    resetPagination();
  };

  const onSubmitFilters = (newFilters: Filters): void => {
    setFilters(newFilters);
    resetPagination();
  };

  const onClearFilters = (): void => {
    setFilters({ ...defaultFilters });
    resetPagination();
  };

  const filteredSessionsData = useMemo((): {
    sessions: SessionInfo[];
    activeFilters: number;
  } => {
    if (!sessions) {
      return { sessions: [], activeFilters: 0 };
    }
    if (!filters) {
      return { sessions, activeFilters: 0 };
    }
    const activeFilters = calculateActiveFilters(filters);
    const timeValue = getTimeValueInSeconds(filters);
    const filteredSessions = sessions
      .filter((s: SessionInfo) => {
        const isInternal = (si: SessionInfo) =>
          si.session.application_name.startsWith(internalAppNamePrefix);
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

    return { sessions: filteredSessions, activeFilters };
  }, [filters, sessions, internalAppNamePrefix]);

  const renderSessions = (): React.ReactElement => {
    const { sessions: sessionsToDisplay, activeFilters } = filteredSessionsData;

    const appNames = getSessionAppFilterOptions(sessions);
    const usernames = getSessionUsernameFilterOptions(sessions);
    const columns = makeSessionsColumns(
      "session",
      terminateSessionRef,
      terminateQueryRef,
    );

    const isColumnSelected = (c: ColumnDescriptor<SessionInfo>) => {
      return (
        (selectedColumns === null && c.showByDefault !== false) ||
        (selectedColumns && selectedColumns.includes(c.name)) ||
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
            sessionStatuses={sessionStatusFilterOptions}
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
                onSubmitColumns={setSelectedColumns}
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
            onChangeSortSetting={onSortClick}
            pagination={pagination}
          />
        </section>
        <Pagination
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={sessionsToDisplay.length}
          onChange={updatePagination}
          onShowSizeChange={updatePagination}
        />
      </>
    );
  };

  return (
    <div className={sessionsPageCx("sessions-page")}>
      <Loading
        loading={sessionsLoading}
        page={"sessions"}
        error={sessionsError}
        render={renderSessions}
        renderError={() =>
          LoadingError({
            statsType: "sessions",
            error: sessionsError,
          })
        }
      />
      <TerminateSessionModal
        ref={terminateSessionRef}
        cancel={(req: CancelSessionRequestMessage) =>
          terminateSession(req).then(
            () => message.success("Session cancelled."),
            () => message.error("There was an error cancelling the session."),
          )
        }
      />
      <TerminateQueryModal
        ref={terminateQueryRef}
        cancel={(req: CancelQueryRequestMessage) =>
          terminateQuery(req).then(
            () => message.success("Statement cancelled."),
            () => message.error("There was an error cancelling the statement."),
          )
        }
      />
    </div>
  );
};

export default SessionsPage;
