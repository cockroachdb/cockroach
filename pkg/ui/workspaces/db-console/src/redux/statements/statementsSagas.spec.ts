// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { expectSaga } from "redux-saga-test-plan";
import { call } from "redux-saga-test-plan/matchers";

import {
  cancelDiagnosticsReportSaga,
  createDiagnosticsReportSaga,
} from "./statementsSagas";
import {
  createStatementDiagnosticsReportCompleteAction,
  createStatementDiagnosticsReportFailedAction,
  createStatementDiagnosticsReportAction,
  cancelStatementDiagnosticsReportCompleteAction,
  cancelStatementDiagnosticsReportFailedAction,
  cancelStatementDiagnosticsReportAction,
} from "./statementsActions";
import {
  cancelStatementDiagnosticsReport,
  createStatementDiagnosticsReport,
} from "src/util/api";
import { cockroach, google } from "src/js/protos";
import CreateStatementDiagnosticsReportRequest = cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;
import CancelStatementDiagnosticsReportRequest = cockroach.server.serverpb.CancelStatementDiagnosticsReportRequest;
import { throwError } from "redux-saga-test-plan/providers";
import Duration = google.protobuf.Duration;
import Long from "long";

describe("statementsSagas", () => {
  describe("requestDiagnostics generator", () => {
    it("calls api#createStatementDiagnosticsReport with statement fingerprint, min exec latency, and expires after fields as payload", () => {
      const statementFingerprint = "some-id";
      const minExecLatency = new Duration({ seconds: new Long(10) });
      const expiresAfter = new Duration({ seconds: new Long(15 * 60) });
      const action = createStatementDiagnosticsReportAction(
        statementFingerprint,
        minExecLatency,
        expiresAfter,
      );
      const createDiagnosticsReportRequest =
        new CreateStatementDiagnosticsReportRequest({
          statement_fingerprint: statementFingerprint,
          min_execution_latency: minExecLatency,
          expires_after: expiresAfter,
        });

      return expectSaga(createDiagnosticsReportSaga, action)
        .provide([
          [call.fn(createStatementDiagnosticsReport), Promise.resolve()],
        ])
        .call(createStatementDiagnosticsReport, createDiagnosticsReportRequest)
        .put(createStatementDiagnosticsReportCompleteAction())
        .dispatch(action)
        .run();
    });
  });

  it("calls dispatched failed action if api#createStatementDiagnosticsReport request failed ", () => {
    const statementFingerprint = "some-id";
    const minExecLatency = new Duration({ seconds: new Long(10) });
    const expiresAfter = new Duration({ seconds: new Long(15 * 60) });
    const action = createStatementDiagnosticsReportAction(
      statementFingerprint,
      minExecLatency,
      expiresAfter,
    );
    const createDiagnosticsReportRequest =
      new CreateStatementDiagnosticsReportRequest({
        statement_fingerprint: statementFingerprint,
        min_execution_latency: minExecLatency,
        expires_after: expiresAfter,
      });

    return expectSaga(createDiagnosticsReportSaga, action)
      .provide([
        [call.fn(createStatementDiagnosticsReport), throwError(new Error())],
      ])
      .call(createStatementDiagnosticsReport, createDiagnosticsReportRequest)
      .put(createStatementDiagnosticsReportFailedAction())
      .dispatch(action)
      .run();
  });

  describe("cancelDiagnostics generator", () => {
    it("calls api#cancelStatementDiagnosticsReport with the diagnostic request ID field as payload", () => {
      const requestID = Long.fromNumber(12345);
      const action = cancelStatementDiagnosticsReportAction(requestID);
      const cancelDiagnosticsReportRequest =
        new CancelStatementDiagnosticsReportRequest({
          request_id: requestID,
        });

      return expectSaga(cancelDiagnosticsReportSaga, action)
        .provide([
          [
            call.fn(cancelStatementDiagnosticsReport),
            Promise.resolve({ error: "" }),
          ],
        ])
        .call(cancelStatementDiagnosticsReport, cancelDiagnosticsReportRequest)
        .put(cancelStatementDiagnosticsReportCompleteAction())
        .dispatch(action)
        .run();
    });
  });

  it("calls dispatched failed action if api#cancelStatementDiagnosticsReport request failed ", () => {
    const requestID = new Long(12345);
    const action = cancelStatementDiagnosticsReportAction(requestID);
    const cancelDiagnosticsReportRequest =
      new CancelStatementDiagnosticsReportRequest({
        request_id: requestID,
      });

    return expectSaga(cancelDiagnosticsReportSaga, action)
      .provide([
        [call.fn(cancelStatementDiagnosticsReport), throwError(new Error())],
      ])
      .call(cancelStatementDiagnosticsReport, cancelDiagnosticsReportRequest)
      .put(cancelStatementDiagnosticsReportFailedAction())
      .dispatch(action)
      .run();
  });
});
