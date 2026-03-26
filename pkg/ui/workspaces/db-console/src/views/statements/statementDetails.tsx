// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import {
  StatementDetails,
  StatementDetailsDispatchProps,
  StatementDetailsStateProps,
  api as clusterUiApi,
} from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";
import { connect } from "react-redux";
import { RouteComponentProps } from "react-router";
import { withRouter } from "react-router-dom";

import { createStatementDiagnosticsAlertLocalSetting } from "src/redux/alerts";
import {
  trackCancelDiagnosticsBundleAction,
  trackDownloadDiagnosticsBundleAction,
  trackStatementDetailsSubnavSelectionAction,
} from "src/redux/analyticsActions";
import { AdminUIState } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import { statementAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";

import { requestTimeLocalSetting } from "./statementsPage";

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): StatementDetailsStateProps => ({
  statementFingerprintID: getMatchParamByName(props.match, statementAttr),
  timeScale: selectTimeScale(state),
  requestTime: requestTimeLocalSetting.selector(state),
});

const mapDispatchToProps: StatementDetailsDispatchProps = {
  dismissStatementDiagnosticsAlertMessage: () =>
    createStatementDiagnosticsAlertLocalSetting.set({ show: false }),
  onTabChanged: trackStatementDetailsSubnavSelectionAction,
  onTimeScaleChange: setGlobalTimeScaleAction,
  onRequestTimeChange: (t: moment.Moment) => requestTimeLocalSetting.set(t),
  onDiagnosticBundleDownload: trackDownloadDiagnosticsBundleAction,
  onDiagnosticCancelRequestTracking: (
    report: clusterUiApi.StatementDiagnosticsReport,
  ) => {
    return (dispatch: any) => {
      dispatch(
        trackCancelDiagnosticsBundleAction(report.statement_fingerprint),
      );
    };
  },
};

export default withRouter(
  connect<
    StatementDetailsStateProps,
    StatementDetailsDispatchProps,
    RouteComponentProps,
    AdminUIState
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(StatementDetails),
);
