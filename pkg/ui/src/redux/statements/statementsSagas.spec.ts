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
import sinon from "sinon";

import { requestDiagnostics } from "./statementsSagas";
import { completeStatementDiagnosticsRequest, requestStatementDiagnostics } from "./statementsActions";
import * as api from "src/util/api";
import { cockroach } from "src/js/protos";
import StatementDiagnosticsRequest = cockroach.server.serverpb.StatementDiagnosticsRequestsRequest;

const sandbox = sinon.createSandbox();

describe("statementsSagas", () => {
  beforeEach(() => {
    sandbox.reset();
  });

  describe("requestDiagnostics generator", () => {
    it("calls api#requestDiagnostics with statement ID as payload", () => {
      const createStatementDiagnosticsRequestStub = sandbox.stub(api, "createStatementDiagnosticsRequest").resolves();
      const statementFingerprint = "some-id";
      const action = requestStatementDiagnostics(statementFingerprint);
      const statementDiagnosticsRequest = new StatementDiagnosticsRequest({ statement_fingerprint: statementFingerprint });

      return expectSaga(requestDiagnostics, action)
        .call(createStatementDiagnosticsRequestStub, statementDiagnosticsRequest)
        .put(completeStatementDiagnosticsRequest())
        .dispatch(action)
        .run();
    });
  });
});
