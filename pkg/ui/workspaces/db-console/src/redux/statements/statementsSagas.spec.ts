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

import { createDiagnosticsReportSaga } from "./statementsSagas";
import {
  createStatementDiagnosticsReportCompleteAction,
  createStatementDiagnosticsReportFailedAction,
  createStatementDiagnosticsReportAction,
} from "./statementsActions";
import { createStatementDiagnosticsReport } from "src/util/api";
import { cockroach, google } from "src/js/protos";
import CreateStatementDiagnosticsReportRequest = cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;
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
      const createDiagnosticsReportRequest = new CreateStatementDiagnosticsReportRequest(
        {
          statement_fingerprint: statementFingerprint,
          min_execution_latency: minExecLatency,
          expires_after: expiresAfter,
        },
      );

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
    const createDiagnosticsReportRequest = new CreateStatementDiagnosticsReportRequest(
      {
        statement_fingerprint: statementFingerprint,
        min_execution_latency: minExecLatency,
        expires_after: expiresAfter,
      },
    );

    return expectSaga(createDiagnosticsReportSaga, action)
      .provide([
        [call.fn(createStatementDiagnosticsReport), throwError(new Error())],
      ])
      .call(createStatementDiagnosticsReport, createDiagnosticsReportRequest)
      .put(createStatementDiagnosticsReportFailedAction())
      .dispatch(action)
      .run();
  });
});
