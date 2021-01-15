import { expectSaga } from "redux-saga-test-plan";
import { throwError } from "redux-saga-test-plan/providers";
import { call } from "redux-saga/effects";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";
import {
  createDiagnosticsReportSaga,
  requestStatementsDiagnosticsSaga,
  StatementDiagnosticsState,
} from "src/store/statementDiagnostics";
import { actions, reducer } from "src/store/statementDiagnostics";
import {
  createStatementDiagnosticsReport,
  getStatementDiagnosticsReports,
} from "src/api/statementDiagnosticsApi";

const CreateStatementDiagnosticsReportResponse =
  cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse;

describe("statementsDiagnostics sagas", () => {
  describe("createDiagnosticsReportSaga", () => {
    const statementFingerprint = "SELECT * FROM table";

    const report = new CreateStatementDiagnosticsReportResponse({
      report: {
        completed: false,
        id: Long.fromNumber(Date.now()),
        statement_fingerprint: statementFingerprint,
      },
    });

    const reportsResponse = new cockroach.server.serverpb.StatementDiagnosticsReportsResponse(
      { reports: [] },
    );

    it("successful request", () => {
      expectSaga(
        createDiagnosticsReportSaga,
        actions.createReport(statementFingerprint),
      )
        .provide([
          [
            call(createStatementDiagnosticsReport, statementFingerprint),
            report,
          ],
          [call(getStatementDiagnosticsReports), reportsResponse],
        ])
        .put(actions.createReportCompleted())
        .put(actions.request())
        .withReducer(reducer)
        .hasFinalState<StatementDiagnosticsState>({
          data: null,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("failed request", () => {
      expectSaga(
        createDiagnosticsReportSaga,
        actions.createReport(statementFingerprint),
      )
        .provide([
          [
            call(createStatementDiagnosticsReport, statementFingerprint),
            throwError(new Error("Failed request")),
          ],
          [call(getStatementDiagnosticsReports), reportsResponse],
        ])
        .put(actions.createReportFailed())
        .run();
    });
  });

  describe("requestStatementsDiagnosticsSaga", () => {
    const statementFingerprint = "SELECT * FROM table";
    const reportsResponse = new cockroach.server.serverpb.StatementDiagnosticsReportsResponse(
      { reports: [{ statement_fingerprint: statementFingerprint }] },
    );

    it("successfully requests diagnostics reports", () => {
      expectSaga(requestStatementsDiagnosticsSaga)
        .provide([[call(getStatementDiagnosticsReports), reportsResponse]])
        .put(actions.received(reportsResponse))
        .withReducer(reducer)
        .hasFinalState<StatementDiagnosticsState>({
          data: reportsResponse,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("fails to request diagnostics reports", () => {
      const error = new Error("Failed request");
      expectSaga(requestStatementsDiagnosticsSaga)
        .provide([[call(getStatementDiagnosticsReports), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<StatementDiagnosticsState>({
          data: null,
          lastError: error,
          valid: false,
        })
        .run();
    });
  });
});
