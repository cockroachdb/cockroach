// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { withRouter } from "react-router-dom";
import { connect } from "react-redux";
import { Dispatch } from "redux";
import { RouteComponentProps } from "react-router-dom";
import {
  StatementDetails,
  StatementDetailsDispatchProps,
} from "./statementDetails";
import { AppState, uiConfigActions } from "../store";
import {
  selectStatementDetails,
  selectStatementDetailsUiConfig,
} from "./statementDetails.selectors";
import {
  selectIsTenant,
  selectHasViewActivityRedactedRole,
  selectHasAdminRole,
} from "../store/uiConfig";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { actions as sqlDetailsStatsActions } from "src/store/statementDetails";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import {
  actions as statementDiagnosticsActions,
  selectDiagnosticsReportsByStatementFingerprint,
} from "src/store/statementDiagnostics";
import { actions as analyticsActions } from "src/store/analytics";
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as nodesActions } from "../store/nodes";
import { actions as nodeLivenessActions } from "../store/liveness";
import { selectTimeScale } from "../store/utils/selectors";
import {
  actions as statementFingerprintInsightActions,
  selectStatementFingerprintInsights,
} from "src/store/insights/statementFingerprintInsights";
import {
  StmtInsightsReq,
  InsertStmtDiagnosticRequest,
  StatementDetailsRequest,
  StatementDiagnosticsReport,
} from "src/api";
import { TimeScale } from "../timeScaleDropdown";
import { getMatchParamByName, statementAttr } from "src/util";
import { selectRequestTime } from "src/statementsPage/statementsPage.selectors";

// For tenant cases, we don't show information about node, regions and
// diagnostics.
const mapStateToProps = (state: AppState, props: RouteComponentProps) => {
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
    diagnosticsReports:
      selectIsTenant(state) || selectHasViewActivityRedactedRole(state)
        ? []
        : selectDiagnosticsReportsByStatementFingerprint(
            state,
            statementFingerprint,
          ),
    uiConfig: selectStatementDetailsUiConfig(state),
    isTenant: selectIsTenant(state),
    hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
    hasAdminRole: selectHasAdminRole(state),
    requestTime: selectRequestTime(state),
    statementFingerprintInsights: selectStatementFingerprintInsights(
      state,
      props,
    ),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): StatementDetailsDispatchProps => ({
  refreshStatementDetails: (req: StatementDetailsRequest) =>
    dispatch(sqlDetailsStatsActions.refresh(req)),
  refreshStatementDiagnosticsRequests: () =>
    dispatch(statementDiagnosticsActions.refresh()),
  refreshNodes: () => dispatch(nodesActions.refresh()),
  refreshNodesLiveness: () => dispatch(nodeLivenessActions.refresh()),
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
  refreshStatementFingerprintInsights: (req: StmtInsightsReq) =>
    dispatch(statementFingerprintInsightActions.refresh(req)),
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
  createStatementDiagnosticsReport: (
    insertStmtDiagnosticsRequest: InsertStmtDiagnosticRequest,
  ) => {
    dispatch(
      statementDiagnosticsActions.createReport(insertStmtDiagnosticsRequest),
    );
    dispatch(
      analyticsActions.track({
        name: "Statement Diagnostics Clicked",
        page: "Statement Details",
        action: "Activated",
      }),
    );
  },
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
  onDiagnosticCancelRequest: (report: StatementDiagnosticsReport) => {
    dispatch(
      statementDiagnosticsActions.cancelReport({
        requestId: report.id,
      }),
    );
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

export const ConnectedStatementDetailsPage = withRouter<any, any>(
  connect(mapStateToProps, mapDispatchToProps)(StatementDetails),
);
