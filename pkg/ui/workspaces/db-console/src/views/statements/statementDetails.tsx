// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { connect } from "react-redux";
import { withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import {
  refreshLiveness,
  refreshNodes,
  refreshStatementDiagnosticsRequests,
  refreshStatementDetails,
  refreshUserSQLRoles,
  generateStmtDetailsToID,
} from "src/redux/apiReducers";
import { RouteComponentProps } from "react-router";
import {
  nodeDisplayNameByIDSelector,
  nodeRegionsByIDSelector,
} from "src/redux/nodes";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { selectDiagnosticsReportsByStatementFingerprint } from "src/redux/statements/statementsSelectors";
import {
  StatementDetails,
  StatementDetailsDispatchProps,
  StatementDetailsStateProps,
} from "@cockroachlabs/cluster-ui";
import {
  cancelStatementDiagnosticsReportAction,
  createStatementDiagnosticsReportAction,
} from "src/redux/statements";
import { createStatementDiagnosticsAlertLocalSetting } from "src/redux/alerts";
import { statementsTimeScaleLocalSetting } from "src/redux/statementsTimeScale";
import { selectHasViewActivityRedactedRole } from "src/redux/user";
import {
  trackCancelDiagnosticsBundleAction,
  trackDownloadDiagnosticsBundleAction,
  trackStatementDetailsSubnavSelectionAction,
} from "src/redux/analyticsActions";
import * as protos from "src/js/protos";
import { StatementDetailsResponseMessage } from "src/util/api";
import { getMatchParamByName, queryByName } from "src/util/query";

import { appNamesAttr, statementAttr } from "src/util/constants";
type IStatementDiagnosticsReport = protos.cockroach.server.serverpb.IStatementDiagnosticsReport;

export const selectStatementDetails = createSelector(
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, statementAttr),
  (_state: AdminUIState, props: RouteComponentProps): string =>
    queryByName(props.location, appNamesAttr),
  (state: AdminUIState) => state.cachedData.statementDetails,
  (
    fingerprintID,
    appNames,
    statementDetailsStats,
  ): StatementDetailsResponseMessage => {
    const key = generateStmtDetailsToID(fingerprintID, appNames);
    if (Object.keys(statementDetailsStats).indexOf(key) >= 0) {
      return statementDetailsStats[key].data;
    }
    return null;
  },
);

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): StatementDetailsStateProps => {
  const completeStatementDetails = selectStatementDetails(state, props);
  const statementFingerprint =
    completeStatementDetails?.statement.key_data.query;
  return {
    completeStatementDetails,
    statementsError: state.cachedData.statements.lastError,
    timeScale: statementsTimeScaleLocalSetting.selector(state),
    nodeNames: nodeDisplayNameByIDSelector(state),
    nodeRegions: nodeRegionsByIDSelector(state),
    diagnosticsReports: selectDiagnosticsReportsByStatementFingerprint(
      state,
      statementFingerprint,
    ),
    hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
  };
};

const mapDispatchToProps: StatementDetailsDispatchProps = {
  refreshStatementDetails,
  refreshStatementDiagnosticsRequests,
  dismissStatementDiagnosticsAlertMessage: () =>
    createStatementDiagnosticsAlertLocalSetting.set({ show: false }),
  createStatementDiagnosticsReport: createStatementDiagnosticsReportAction,
  onTabChanged: trackStatementDetailsSubnavSelectionAction,
  onDiagnosticBundleDownload: trackDownloadDiagnosticsBundleAction,
  onDiagnosticCancelRequest: (report: IStatementDiagnosticsReport) => {
    return (dispatch: AppDispatch) => {
      dispatch(cancelStatementDiagnosticsReportAction(report.id));
      dispatch(
        trackCancelDiagnosticsBundleAction(report.statement_fingerprint),
      );
    };
  },
  refreshNodes: refreshNodes,
  refreshNodesLiveness: refreshLiveness,
  refreshUserSQLRoles: refreshUserSQLRoles,
};

export default withRouter(
  connect<StatementDetailsStateProps, StatementDetailsDispatchProps>(
    mapStateToProps,
    mapDispatchToProps,
  )(StatementDetails),
);
