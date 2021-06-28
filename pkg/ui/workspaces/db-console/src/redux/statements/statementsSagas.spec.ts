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
import { cockroach } from "src/js/protos";
import CreateStatementDiagnosticsReportRequest = cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;
import { throwError } from "redux-saga-test-plan/providers";

describe("statementsSagas", () => {
  describe("requestDiagnostics generator", () => {
    it("calls api#createStatementDiagnosticsReport with statement fingerprint as payload", () => {
      const statementFingerprint = "some-id";
      const action = createStatementDiagnosticsReportAction(
        statementFingerprint,
      );
      const diagnosticsReportRequest = new CreateStatementDiagnosticsReportRequest(
        {
          statement_fingerprint: statementFingerprint,
        },
      );

      return expectSaga(createDiagnosticsReportSaga, action)
        .provide([
          [call.fn(createStatementDiagnosticsReport), Promise.resolve()],
        ])
        .call(createStatementDiagnosticsReport, diagnosticsReportRequest)
        .put(createStatementDiagnosticsReportCompleteAction())
        .dispatch(action)
        .run();
    });
  });

  it("calls dispatched failed action if api#createStatementDiagnosticsReport request failed ", () => {
    const statementFingerprint = "some-id";
    const action = createStatementDiagnosticsReportAction(statementFingerprint);
    const diagnosticsReportRequest = new CreateStatementDiagnosticsReportRequest(
      {
        statement_fingerprint: statementFingerprint,
      },
    );

    return expectSaga(createDiagnosticsReportSaga, action)
      .provide([
        [call.fn(createStatementDiagnosticsReport), throwError(new Error())],
      ])
      .call(createStatementDiagnosticsReport, diagnosticsReportRequest)
      .put(createStatementDiagnosticsReportFailedAction())
      .dispatch(action)
      .run();
  });
});
