import { withRouter } from "react-router-dom";
import { connect } from "react-redux";
import { Dispatch } from "redux";
import {
  StatementDetails,
  StatementDetailsDispatchProps,
  StatementDetailsProps,
  StatementDetailsStateProps,
} from "./statementDetails";
import { AppState } from "../store";
import {
  selectStatement,
  selectStatementDetailsUiConfig,
} from "./statementDetails.selectors";
import { nodeDisplayNameByIDSelector } from "../store/nodes";
import { actions as statementActions } from "src/store/statements";
import {
  actions as statementDiagnosticsActions,
  selectDiagnosticsReportsByStatementFingerprint,
} from "src/store/statementDiagnostics";
import { actions as analyticsActions } from "src/store/analytics";
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as nodesActions } from "../store/nodes";
import { actions as nodeLivenessActions } from "../store/liveness";

const mapStateToProps = (
  state: AppState,
  props: StatementDetailsProps,
): StatementDetailsStateProps => {
  const statement = selectStatement(state, props);
  const statementFingerprint = statement?.statement;
  return {
    statement,
    statementsError: state.adminUI.statements.lastError,
    nodeNames: nodeDisplayNameByIDSelector(state),
    diagnosticsReports: selectDiagnosticsReportsByStatementFingerprint(
      state,
      statementFingerprint,
    ),
    uiConfig: selectStatementDetailsUiConfig(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): StatementDetailsDispatchProps => ({
  refreshStatements: () => dispatch(statementActions.refresh()),
  refreshStatementDiagnosticsRequests: () =>
    dispatch(statementDiagnosticsActions.refresh()),
  refreshNodes: () => dispatch(nodesActions.refresh()),
  refreshNodesLiveness: () => dispatch(nodeLivenessActions.refresh()),
  dismissStatementDiagnosticsAlertMessage: () =>
    dispatch(
      localStorageActions.update({
        key: "adminUi/showDiagnosticsModal",
        value: false,
      }),
    ),
  createStatementDiagnosticsReport: (statementFingerprint: string) => {
    dispatch(statementDiagnosticsActions.createReport(statementFingerprint));
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
  onSortingChange: (tableName, columnName) =>
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Statement Details",
        columnName,
        tableName,
      }),
    ),
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
