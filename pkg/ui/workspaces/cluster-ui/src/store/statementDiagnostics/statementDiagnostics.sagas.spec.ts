// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import { call } from "redux-saga/effects";
import { expectSaga } from "redux-saga-test-plan";
import { throwError } from "redux-saga-test-plan/providers";

import {
  createStatementDiagnosticsReport,
  cancelStatementDiagnosticsReport,
  getStatementDiagnosticsReports,
  InsertStmtDiagnosticRequest,
  InsertStmtDiagnosticResponse,
  StatementDiagnosticsResponse,
  CancelStmtDiagnosticRequest,
  CancelStmtDiagnosticResponse,
} from "src/api/statementDiagnosticsApi";
import {
  createDiagnosticsReportSaga,
  requestStatementsDiagnosticsSaga,
  cancelDiagnosticsReportSaga,
  StatementDiagnosticsState,
  actions,
  reducer,
} from "src/store/statementDiagnostics";

describe("statementsDiagnostics sagas", () => {
  describe("createDiagnosticsReportSaga", () => {
    const statementFingerprint = "SELECT * FROM table";
    const planGist = "gist";
    const minExecLatency = 100; // num seconds
    const expiresAfter = 0; // num seconds, setting expiresAfter to 0 means the request won't expire.

    const insertRequest: InsertStmtDiagnosticRequest = {
      stmtFingerprint: statementFingerprint,
      minExecutionLatencySeconds: minExecLatency,
      expiresAfterSeconds: expiresAfter,
      planGist: planGist,
      redacted: false,
    };

    const insertResponse: InsertStmtDiagnosticResponse = {
      req_resp: true,
    };

    const reportsResponse: StatementDiagnosticsResponse = [];

    it("successful request", () => {
      expectSaga(
        createDiagnosticsReportSaga,
        actions.createReport(insertRequest),
      )
        .provide([
          [
            call(createStatementDiagnosticsReport, insertRequest),
            insertResponse,
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
      const error = new Error("Failed request");
      expectSaga(
        createDiagnosticsReportSaga,
        actions.createReport(insertRequest),
      )
        .provide([
          [
            call(createStatementDiagnosticsReport, insertRequest),
            throwError(error),
          ],
          [call(getStatementDiagnosticsReports), reportsResponse],
        ])
        .put(actions.createReportFailed(error))
        .run();
    });
  });

  describe("requestStatementsDiagnosticsSaga", () => {
    const statementFingerprint = "SELECT * FROM table";
    const requestedAt = moment.now();
    const expiresAt = 100; // num seconds
    const minExecutionLatency = 10; // num seconds

    const statementDiagnosticsResponse: StatementDiagnosticsResponse = [
      {
        id: "12345367789654",
        statement_diagnostics_id: "12367543632253",
        completed: false,
        requested_at: moment(requestedAt),
        expires_at: moment(requestedAt + expiresAt),
        min_execution_latency: moment.duration(minExecutionLatency, "s"),
        statement_fingerprint: statementFingerprint,
      },
    ];

    it("successfully requests diagnostics reports", () => {
      expectSaga(requestStatementsDiagnosticsSaga)
        .provide([
          [call(getStatementDiagnosticsReports), statementDiagnosticsResponse],
        ])
        .put(actions.received(statementDiagnosticsResponse))
        .withReducer(reducer)
        .hasFinalState<StatementDiagnosticsState>({
          data: statementDiagnosticsResponse,
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

  describe("cancelDiagnosticsReportSaga", () => {
    const cancelRequest: CancelStmtDiagnosticRequest = {
      requestId: "123456789876",
    };
    const cancelResponse: CancelStmtDiagnosticResponse = {
      stmt_diag_req_id: "123456789876",
    };
    const reportsResponse: StatementDiagnosticsResponse = [];

    it("successful request", () => {
      return expectSaga(
        cancelDiagnosticsReportSaga,
        actions.cancelReport(cancelRequest),
      )
        .provide([
          [
            call(cancelStatementDiagnosticsReport, cancelRequest),
            cancelResponse,
          ],
          [call(getStatementDiagnosticsReports), reportsResponse],
        ])
        .put(actions.cancelReportCompleted())
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
      const error = new Error("Failed request");
      return expectSaga(
        cancelDiagnosticsReportSaga,
        actions.cancelReport(cancelRequest),
      )
        .provide([
          [
            call(cancelStatementDiagnosticsReport, cancelRequest),
            throwError(error),
          ],
          [call(getStatementDiagnosticsReports), reportsResponse],
        ])
        .put(actions.cancelReportFailed(error))
        .run();
    });
  });
});
