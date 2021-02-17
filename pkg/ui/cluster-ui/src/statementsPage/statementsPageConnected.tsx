import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { AppState } from "src/store";
import { actions as statementActions } from "src/store/statements";
import { actions as statementDiagnosticsActions } from "src/store/statementDiagnostics";
import { actions as analyticsActions } from "src/store/analytics";
import { actions as localStorageActions } from "src/store/localStorage";
import {
  StatementsPage,
  StatementsPageDispatchProps,
  StatementsPageOuterProps,
  StatementsPageProps,
  StatementsPageStateProps,
} from "./statementsPage";
import {
  selectApps,
  selectLastReset,
  selectStatements,
  selectStatementsLastError,
  selectTotalFingerprints,
} from "./statementsPage.selectors";
import { AggregateStatistics } from "../statementsTable";

type OwnProps = StatementsPageOuterProps & RouteComponentProps;
export const ConnectedStatementsPage = withRouter(
  connect<StatementsPageStateProps, StatementsPageDispatchProps, OwnProps>(
    (state: AppState, props: StatementsPageProps) => ({
      statements: selectStatements(state, props),
      statementsError: selectStatementsLastError(state),
      apps: selectApps(state),
      totalFingerprints: selectTotalFingerprints(state),
      lastReset: selectLastReset(state),
    }),
    {
      refreshStatements: statementActions.refresh,
      refreshStatementDiagnosticsRequests: statementDiagnosticsActions.refresh,
      dismissAlertMessage: () =>
        localStorageActions.update({
          key: "adminUi/showDiagnosticsModal",
          value: false,
        }),
      onActivateStatementDiagnostics: statementDiagnosticsActions.createReport,
      onDiagnosticsModalOpen: (statementFingerprint: string) =>
        analyticsActions.activateDiagnostics({
          page: "statements",
          value: statementFingerprint,
        }),
      onSearchComplete: (results: AggregateStatistics[]) =>
        analyticsActions.search({
          page: "statements",
          value: results?.length || 0,
        }),
      onPageChanged: (pageNum: number) =>
        analyticsActions.pagination({ page: "statements", value: pageNum }),
      onSortingChange: (
        tableName: string,
        columnName: string,
        ascending: boolean,
      ) =>
        analyticsActions.sorting({
          page: "statements",
          value: {
            tableName,
            columnName,
            ascending,
          },
        }),
    },
  )(StatementsPage),
);
