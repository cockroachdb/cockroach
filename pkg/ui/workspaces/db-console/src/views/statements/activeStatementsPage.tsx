// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  ActiveStatementFilters,
  ActiveStatement,
  getActiveStatementsFromSessions,
  ActiveStatementsView,
  ActiveStatementsViewDispatchProps,
  ActiveStatementsViewStateProps,
  defaultFilters,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { SessionsResponseMessage } from "src/util/api";
import { refreshSessions } from "src/redux/apiReducers";

const selectActiveStatements = createSelector(
  (state: AdminUIState) => state.cachedData.sessions,
  (
    state?: CachedDataReducerState<SessionsResponseMessage>,
  ): ActiveStatement[] => {
    if (state == null || state.data == null) return [];
    return getActiveStatementsFromSessions(state.data);
  },
);

const statementColumnsLocalSetting = new LocalSetting<
  AdminUIState,
  string | null
>(
  "columns/ActiveStatementsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

const filtersLocalSetting = new LocalSetting<
  AdminUIState,
  ActiveStatementFilters
>(
  "filters/ActiveStatementsPage",
  (state: AdminUIState) => state.localSettings,
  { app: defaultFilters.app },
);

const sortSettingLocalSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/ActiveStatementsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "startTime" },
);

export default withRouter(
  connect<
    ActiveStatementsViewStateProps,
    ActiveStatementsViewDispatchProps,
    RouteComponentProps
  >(
    (state: AdminUIState) => ({
      filters: filtersLocalSetting.selector(state),
      selectedColumns: statementColumnsLocalSetting.selectorToArray(state),
      sortSetting: sortSettingLocalSetting.selector(state),
      statements: selectActiveStatements(state),
      fetchError: state.cachedData?.sessions.lastError,
    }),
    {
      onColumnsSelect: (columns: string[]) => {
        statementColumnsLocalSetting.set(
          columns.length === 0 ? " " : columns.join(","),
        );
      },
      refreshSessions,
      onFiltersChange: (filters: ActiveStatementFilters) =>
        filtersLocalSetting.set(filters),
      onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
    },
  )(ActiveStatementsView),
);
