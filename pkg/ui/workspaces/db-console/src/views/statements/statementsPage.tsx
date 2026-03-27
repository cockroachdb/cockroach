// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Filters,
  defaultFilters,
  StatementsPageRoot,
  StatementsPageStateProps,
  StatementsPageDispatchProps,
  StatementsPageRootProps,
  api,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { bindActionCreators } from "redux";

import {
  trackApplySearchCriteriaAction,
  trackCancelDiagnosticsBundleAction,
  trackDownloadDiagnosticsBundleAction,
  trackStatementsPaginationAction,
} from "src/redux/analyticsActions";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import {
  trackActivateDiagnostics,
  trackDiagnosticsModalOpen,
} from "src/util/analytics";

export const statementColumnsLocalSetting = new LocalSetting(
  "create_statement_columns",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const sortSettingLocalSetting = new LocalSetting(
  "tableSortSetting/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "executionCount" },
);

export const requestTimeLocalSetting = new LocalSetting(
  "requestTime/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const filtersLocalSetting = new LocalSetting<AdminUIState, Filters>(
  "filters/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  defaultFilters,
);

export const searchLocalSetting = new LocalSetting(
  "search/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const reqSortSetting = new LocalSetting(
  "reqSortSetting/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  api.DEFAULT_STATS_REQ_OPTIONS.sortStmt,
);

export const limitSetting = new LocalSetting(
  "reqLimitSetting/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  api.DEFAULT_STATS_REQ_OPTIONS.limit,
);

const fingerprintsPageActions = {
  onTimeScaleChange: setGlobalTimeScaleAction,
  onActivateStatementDiagnosticsAnalytics: (statementFingerprint: string) => {
    return () => trackActivateDiagnostics(statementFingerprint);
  },
  onDiagnosticsModalOpenAnalytics: (statement: string) => {
    return () => trackDiagnosticsModalOpen(statement);
  },
  onDiagnosticsReportDropdownOptionAnalytics: (
    report: api.StatementDiagnosticsReport,
  ) => {
    if (report.completed) {
      return trackDownloadDiagnosticsBundleAction(report.statement_fingerprint);
    } else {
      return trackCancelDiagnosticsBundleAction(report.statement_fingerprint);
    }
  },
  onSearchComplete: (query: string) => searchLocalSetting.set(query),
  onPageChanged: trackStatementsPaginationAction,
  onSortingChange: (
    _tableName: string,
    columnName: string,
    ascending: boolean,
  ) =>
    sortSettingLocalSetting.set({
      ascending: ascending,
      columnTitle: columnName,
    }),
  onRequestTimeChange: (t: moment.Moment) => requestTimeLocalSetting.set(t),
  onFilterChange: (filters: Filters) => filtersLocalSetting.set(filters),
  onColumnsChange: (value: string[]) =>
    statementColumnsLocalSetting.set(
      value.length === 0 ? " " : value.join(","),
    ),
  onChangeLimit: (newLimit: number) => limitSetting.set(newLimit),
  onChangeReqSort: (sort: api.SqlStatsSortType) => reqSortSetting.set(sort),
  onApplySearchCriteria: trackApplySearchCriteriaAction,
};

type StateProps = {
  fingerprintsPageProps: StatementsPageStateProps;
};

type DispatchProps = {
  fingerprintsPageProps: StatementsPageDispatchProps;
};

export default withRouter(
  connect<
    StateProps,
    DispatchProps,
    RouteComponentProps,
    StatementsPageRootProps,
    AdminUIState
  >(
    (state: AdminUIState, props: RouteComponentProps) => ({
      fingerprintsPageProps: {
        ...props,
        columns: statementColumnsLocalSetting.selectorToArray(state),
        timeScale: selectTimeScale(state),
        filters: filtersLocalSetting.selector(state),
        search: searchLocalSetting.selector(state),
        sortSetting: sortSettingLocalSetting.selector(state),
        requestTime: requestTimeLocalSetting.selector(state),
        limit: limitSetting.selector(state),
        reqSortSetting: reqSortSetting.selector(state),
      },
    }),
    (dispatch: AppDispatch): DispatchProps => ({
      fingerprintsPageProps: bindActionCreators(
        fingerprintsPageActions,
        dispatch,
      ),
    }),
    (
      stateProps: StateProps,
      dispatchProps: DispatchProps,
    ): StatementsPageRootProps => ({
      fingerprintsPageProps: {
        ...stateProps.fingerprintsPageProps,
        ...dispatchProps.fingerprintsPageProps,
      },
    }),
  )(StatementsPageRoot),
);
