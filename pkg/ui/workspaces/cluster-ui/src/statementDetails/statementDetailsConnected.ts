// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import { connect } from "react-redux";
import { withRouter, RouteComponentProps } from "react-router-dom";
import { Dispatch } from "redux";

import { StatementDiagnosticsReport } from "src/api";
import { selectRequestTime } from "src/statementsPage/statementsPage.selectors";
import { actions as analyticsActions } from "src/store/analytics";
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import { getMatchParamByName, statementAttr } from "src/util";

import { AppState } from "../store";
import { selectTimeScale } from "../store/utils/selectors";
import { TimeScale } from "../timeScaleDropdown";

import {
  StatementDetails,
  StatementDetailsDispatchProps,
} from "./statementDetails";

const mapStateToProps = (state: AppState, props: RouteComponentProps) => ({
  statementFingerprintID: getMatchParamByName(props.match, statementAttr),
  timeScale: selectTimeScale(state),
  requestTime: selectRequestTime(state),
});

const mapDispatchToProps = (
  dispatch: Dispatch,
): StatementDetailsDispatchProps => ({
  onTimeScaleChange: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "TimeScale changed",
        page: "Statement Details",
        value: ts.key,
      }),
    );
  },
  dismissStatementDiagnosticsAlertMessage: () =>
    dispatch(
      localStorageActions.update({
        key: "adminUi/showDiagnosticsModal",
        value: false,
      }),
    ),
  onTabChanged: tabName =>
    dispatch(
      analyticsActions.track({
        name: "Tab Changed",
        page: "Statement Details",
        tabName,
      }),
    ),
  onDiagnosticBundleDownload: () =>
    dispatch(
      analyticsActions.track({
        name: "Statement Diagnostics Clicked",
        page: "Statement Details",
        action: "Downloaded",
      }),
    ),
  onDiagnosticCancelRequestTracking: (_report: StatementDiagnosticsReport) => {
    dispatch(
      analyticsActions.track({
        name: "Statement Diagnostics Clicked",
        page: "Statement Details",
        action: "Cancelled",
      }),
    );
  },
  onSortingChange: (tableName, columnName) =>
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Statement Details",
        columnName,
        tableName,
      }),
    ),
  onRequestTimeChange: (t: moment.Moment) => {
    dispatch(
      localStorageActions.update({
        key: "requestTime/StatementsPage",
        value: t,
      }),
    );
  },
  onBackToStatementsClick: () =>
    dispatch(
      analyticsActions.track({
        name: "Back Clicked",
        page: "Statement Details",
      }),
    ),
});

export const ConnectedStatementDetailsPage = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(StatementDetails),
);
