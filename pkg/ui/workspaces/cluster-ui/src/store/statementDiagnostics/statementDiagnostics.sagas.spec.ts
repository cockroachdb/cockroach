// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { expectSaga } from "redux-saga-test-plan";
import { throwError } from "redux-saga-test-plan/providers";
import { call } from "redux-saga/effects";
import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";
import {
  createDiagnosticsReportSaga,
  requestStatementsDiagnosticsSaga,
  cancelDiagnosticsReportSaga,
  StatementDiagnosticsState,
} from "src/store/statementDiagnostics";
import { actions, reducer } from "src/store/statementDiagnostics";
import {
  createStatementDiagnosticsReport,
  cancelStatementDiagnosticsReport,
  getStatementDiagnosticsReports,
} from "src/api/statementDiagnosticsApi";

const CreateStatementDiagnosticsReportRequest =
  cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;
const CreateStatementDiagnosticsReportResponse =
  cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse;

const CancelStatementDiagnosticsReportRequest =
  cockroach.server.serverpb.CancelStatementDiagnosticsReportRequest;
const CancelStatementDiagnosticsReportResponse =
  cockroach.server.serverpb.CancelStatementDiagnosticsReportResponse;

describe("statementsDiagnostics sagas", () => {
  describe("createDiagnosticsReportSaga", () => {
    const statementFingerprint = "SELECT * FROM table";
    const minExecLatency = new google.protobuf.Duration({
      seconds: new Long(100),
    });
    const expiresAfter = new google.protobuf.Duration({
      seconds: new Long(0), // Setting expiresAfter to 0 means the request won't expire.
    });

    const request = new CreateStatementDiagnosticsReportRequest({
      statement_fingerprint: statementFingerprint,
      min_execution_latency: minExecLatency,
      expires_after: expiresAfter,
    });

    const report = new CreateStatementDiagnosticsReportResponse({
      report: {
        completed: false,
        id: Long.fromNumber(Date.now()),
        statement_fingerprint: statementFingerprint,
      },
    });

    const reportsResponse =
      new cockroach.server.serverpb.StatementDiagnosticsReportsResponse({
        reports: [],
      });

    it("successful request", () => {
      expectSaga(createDiagnosticsReportSaga, actions.createReport(request))
        .provide([
          [call(createStatementDiagnosticsReport, request), report],
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
      expectSaga(createDiagnosticsReportSaga, actions.createReport(request))
        .provide([
          [call(createStatementDiagnosticsReport, request), throwError(error)],
          [call(getStatementDiagnosticsReports), reportsResponse],
        ])
        .put(actions.createReportFailed(error))
        .run();
    });
  });

  describe("requestStatementsDiagnosticsSaga", () => {
    const statementFingerprint = "SELECT * FROM table";
    const reportsResponse =
      new cockroach.server.serverpb.StatementDiagnosticsReportsResponse({
        reports: [{ statement_fingerprint: statementFingerprint }],
      });

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

  describe("cancelDiagnosticsReportSaga", () => {
    const requestID = Long.fromNumber(12345);
    const request = new CancelStatementDiagnosticsReportRequest({
      request_id: requestID,
    });

    const report = new CancelStatementDiagnosticsReportResponse({
      canceled: true,
      error: "",
    });

    const reportsResponse =
      new cockroach.server.serverpb.StatementDiagnosticsReportsResponse({
        reports: [],
      });

    it("successful request", () => {
      return expectSaga(
        cancelDiagnosticsReportSaga,
        actions.cancelReport(request),
      )
        .provide([
          [call(cancelStatementDiagnosticsReport, request), report],
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
        actions.cancelReport(request),
      )
        .provide([
          [call(cancelStatementDiagnosticsReport, request), throwError(error)],
          [call(getStatementDiagnosticsReports), reportsResponse],
        ])
        .put(actions.cancelReportFailed(error))
        .run();
    });
  });
});
