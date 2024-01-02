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
import Long from "long";

import {
  refreshLiveness,
  refreshNodes,
  refreshStatementDiagnosticsRequests,
  refreshStatementDetails,
  refreshUserSQLRoles,
  refreshStatementFingerprintInsights,
} from "src/redux/apiReducers";
import { RouteComponentProps } from "react-router";
import { nodeRegionsByIDSelector } from "src/redux/nodes";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { selectDiagnosticsReportsByStatementFingerprint } from "src/redux/statements/statementsSelectors";
import {
  StatementDetails,
  StatementDetailsDispatchProps,
  StatementDetailsStateProps,
  toRoundedDateRange,
  util,
} from "@cockroachlabs/cluster-ui";
import {
  cancelStatementDiagnosticsReportAction,
  createStatementDiagnosticsReportAction,
  setGlobalTimeScaleAction,
} from "src/redux/statements";
import { createStatementDiagnosticsAlertLocalSetting } from "src/redux/alerts";
import {
  selectHasAdminRole,
  selectHasViewActivityRedactedRole,
} from "src/redux/user";
import {
  trackCancelDiagnosticsBundleAction,
  trackDownloadDiagnosticsBundleAction,
  trackStatementDetailsSubnavSelectionAction,
} from "src/redux/analyticsActions";
import { StatementDetailsResponseMessage } from "src/util/api";
import { getMatchParamByName, queryByName } from "src/util/query";

import { appNamesAttr, statementAttr } from "src/util/constants";
import { selectTimeScale } from "src/redux/timeScale";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";
import { requestTimeLocalSetting } from "./statementsPage";

const { generateStmtDetailsToID } = util;

export const selectStatementDetails = createSelector(
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, statementAttr),
  (_state: AdminUIState, props: RouteComponentProps): string =>
    queryByName(props.location, appNamesAttr),
  selectTimeScale,
  (state: AdminUIState) => state.cachedData.statementDetails,
  (
    fingerprintID,
    appNames,
    timeScale,
    statementDetailsStats,
  ): {
    statementDetails: StatementDetailsResponseMessage;
    isLoading: boolean;
    lastError: Error;
    lastUpdated: moment.Moment | null;
  } => {
    // Since the aggregation interval is 1h, we want to round the selected timeScale to include
    // the full hour. If a timeScale is between 14:32 - 15:17 we want to search for values
    // between 14:00 - 16:00. We don't encourage the aggregation interval to be modified, but
    // in case that changes in the future we might consider changing this function to use the
    // cluster settings value for the rounding function.
    const [start, end] = toRoundedDateRange(timeScale);
    const key = generateStmtDetailsToID(
      fingerprintID,
      appNames,
      Long.fromNumber(start.unix()),
      Long.fromNumber(end.unix()),
    );
    if (Object.keys(statementDetailsStats).includes(key)) {
      return {
        statementDetails: statementDetailsStats[key].data,
        isLoading: statementDetailsStats[key].inFlight,
        lastError: statementDetailsStats[key].lastError,
        lastUpdated: statementDetailsStats[key]?.setAt?.utc(),
      };
    }
    return {
      statementDetails: null,
      isLoading: true,
      lastError: null,
      lastUpdated: null,
    };
  },
);

const selectStatementFingerprintInsights = createSelector(
  (state: AdminUIState) => state.cachedData.statementFingerprintInsights,
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, statementAttr),
  (cachedFingerprintInsights, fingerprintID) => {
    return cachedFingerprintInsights[fingerprintID]?.data?.results;
  },
);

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): StatementDetailsStateProps => {
  const { statementDetails, isLoading, lastError, lastUpdated } =
    selectStatementDetails(state, props);
  const statementFingerprint = statementDetails?.statement.metadata.query;
  return {
    statementFingerprintID: getMatchParamByName(props.match, statementAttr),
    statementDetails,
    isLoading: isLoading,
    statementsError: lastError,
    lastUpdated: lastUpdated,
    timeScale: selectTimeScale(state),
    nodeRegions: nodeRegionsByIDSelector(state),
    diagnosticsReports: selectDiagnosticsReportsByStatementFingerprint(
      state,
      statementFingerprint,
    ),
    hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
    hasAdminRole: selectHasAdminRole(state),
    requestTime: requestTimeLocalSetting.selector(state),
    statementFingerprintInsights: selectStatementFingerprintInsights(
      state,
      props,
    ),
  };
};

const mapDispatchToProps: StatementDetailsDispatchProps = {
  refreshStatementDetails,
  refreshStatementDiagnosticsRequests,
  dismissStatementDiagnosticsAlertMessage: () =>
    createStatementDiagnosticsAlertLocalSetting.set({ show: false }),
  createStatementDiagnosticsReport: (
    insertStatementDiagnosticsRequest: clusterUiApi.InsertStmtDiagnosticRequest,
  ) => {
    return (dispatch: AppDispatch) => {
      dispatch(
        createStatementDiagnosticsReportAction(
          insertStatementDiagnosticsRequest,
        ),
      );
    };
  },
  onTabChanged: trackStatementDetailsSubnavSelectionAction,
  onTimeScaleChange: setGlobalTimeScaleAction,
  onRequestTimeChange: (t: moment.Moment) => requestTimeLocalSetting.set(t),
  onDiagnosticBundleDownload: trackDownloadDiagnosticsBundleAction,
  onDiagnosticCancelRequest: (
    report: clusterUiApi.StatementDiagnosticsReport,
  ) => {
    return (dispatch: AppDispatch) => {
      dispatch(
        cancelStatementDiagnosticsReportAction({ requestId: report.id }),
      );
      dispatch(
        trackCancelDiagnosticsBundleAction(report.statement_fingerprint),
      );
    };
  },
  refreshNodes: refreshNodes,
  refreshNodesLiveness: refreshLiveness,
  refreshUserSQLRoles: refreshUserSQLRoles,
  refreshStatementFingerprintInsights: (req: clusterUiApi.StmtInsightsReq) =>
    refreshStatementFingerprintInsights(req),
};

export default withRouter(
  connect<StatementDetailsStateProps, StatementDetailsDispatchProps>(
    mapStateToProps,
    mapDispatchToProps,
  )(StatementDetails),
);
