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
import { throwError } from "redux-saga-test-plan/providers";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";

describe("statementsSagas", () => {
  describe("requestDiagnostics generator", () => {
    it("calls api#createStatementDiagnosticsReport with statement fingerprint, min exec latency, and expires after fields as payload", () => {
      const statementFingerprint = "some-id";
      const minExecLatency = 10; // num seconds
      const expiresAfter = 15 * 60; // num seconds (num mins * num seconds per min)
      const insertStmtDiagnosticsRequest = {
        stmtFingerprint: statementFingerprint,
        minExecutionLatencySeconds: minExecLatency,
        expiresAfterSeconds: expiresAfter,
      };
      const action = createStatementDiagnosticsReportAction(
        insertStmtDiagnosticsRequest,
      );
      return expectSaga(createDiagnosticsReportSaga, action)
        .provide([
          [
            call.fn(clusterUiApi.createStatementDiagnosticsReport),
            Promise.resolve(),
          ],
        ])
        .call(
          clusterUiApi.createStatementDiagnosticsReport,
          insertStmtDiagnosticsRequest,
        )
        .put(createStatementDiagnosticsReportCompleteAction())
        .dispatch(action)
        .run();
    });
  });

  it("calls dispatched failed action if api#createStatementDiagnosticsReport request failed ", () => {
    const statementFingerprint = "some-id";
    const minExecLatency = 10; // num seconds
    const expiresAfter = 15 * 60; // num seconds (num mins * num seconds per min)
    const insertStmtDiagnosticsRequest = {
      stmtFingerprint: statementFingerprint,
      minExecutionLatencySeconds: minExecLatency,
      expiresAfterSeconds: expiresAfter,
    };
    const action = createStatementDiagnosticsReportAction(
      insertStmtDiagnosticsRequest,
    );

    return expectSaga(createDiagnosticsReportSaga, action)
      .provide([
        [
          call.fn(clusterUiApi.createStatementDiagnosticsReport),
          throwError(new Error()),
        ],
      ])
      .call(
        clusterUiApi.createStatementDiagnosticsReport,
        insertStmtDiagnosticsRequest,
      )
      .put(createStatementDiagnosticsReportFailedAction())
      .dispatch(action)
      .run();
  });

  describe("cancelDiagnostics generator", () => {
    it("calls api#cancelStatementDiagnosticsReport with the diagnostic request ID field as payload", () => {
      const requestId = "810501245312335873";
      const cancelDiagnosticsReportRequest = { requestId };
      const action = cancelStatementDiagnosticsReportAction(
        cancelDiagnosticsReportRequest,
      );

      return expectSaga(cancelDiagnosticsReportSaga, action)
        .provide([
          [
            call.fn(clusterUiApi.cancelStatementDiagnosticsReport),
            Promise.resolve({ error: "" }),
          ],
        ])
        .call(
          clusterUiApi.cancelStatementDiagnosticsReport,
          cancelDiagnosticsReportRequest,
        )
        .put(cancelStatementDiagnosticsReportCompleteAction())
        .dispatch(action)
        .run();
    });
  });

  it("calls dispatched failed action if api#cancelStatementDiagnosticsReport request failed ", () => {
    const requestId = "810501245312335873";
    const cancelDiagnosticsReportRequest = { requestId };
    const action = cancelStatementDiagnosticsReportAction(
      cancelDiagnosticsReportRequest,
    );

    return expectSaga(cancelDiagnosticsReportSaga, action)
      .provide([
        [
          call.fn(clusterUiApi.cancelStatementDiagnosticsReport),
          throwError(new Error()),
        ],
      ])
      .call(
        clusterUiApi.cancelStatementDiagnosticsReport,
        cancelDiagnosticsReportRequest,
      )
      .put(cancelStatementDiagnosticsReportFailedAction())
      .dispatch(action)
      .run();
  });
});
