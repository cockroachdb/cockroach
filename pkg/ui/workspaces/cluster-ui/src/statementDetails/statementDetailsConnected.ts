// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { withRouter, RouteComponentProps } from "react-router-dom";
import { Dispatch } from "redux";

import {
  StmtInsightsReq,
  InsertStmtDiagnosticRequest,
  StatementDetailsRequest,
  StatementDiagnosticsReport,
} from "src/api";
import { selectRequestTime } from "src/statementsPage/statementsPage.selectors";
import { actions as analyticsActions } from "src/store/analytics";
import {
  actions as statementFingerprintInsightActions,
  selectStatementFingerprintInsights,
} from "src/store/insights/statementFingerprintInsights";
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import { actions as sqlDetailsStatsActions } from "src/store/statementDetails";
import {
  actions as statementDiagnosticsActions,
  selectDiagnosticsReportsByStatementFingerprint,
} from "src/store/statementDiagnostics";
import { getMatchParamByName, statementAttr } from "src/util";

import { AppState, uiConfigActions } from "../store";
import { actions as nodeLivenessActions } from "../store/liveness";
import {
  nodeRegionsByIDSelector,
  actions as nodesActions,
} from "../store/nodes";
import {
  selectIsTenant,
  selectHasViewActivityRedactedRole,
  selectHasAdminRole,
} from "../store/uiConfig";
import { selectTimeScale } from "../store/utils/selectors";
import { TimeScale } from "../timeScaleDropdown";

import {
  StatementDetails,
  StatementDetailsDispatchProps,
} from "./statementDetails";
import {
  selectStatementDetails,
  selectStatementDetailsUiConfig,
} from "./statementDetails.selectors";

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
    diagnosticsReports: selectHasViewActivityRedactedRole(state)
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

export const ConnectedStatementDetailsPage = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(StatementDetails),
);
