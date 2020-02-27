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

import { requestDiagnostics } from "./statementsSagas";
import { completeEnqueueDiagnostics, enqueueDiagnostics } from "./statementsActions";
import { enqueueStatementDiagnostics } from "src/util/api";

describe("statementsSagas", () => {
  describe("requestDiagnostics generator", () => {
    it("calls api#requestDiagnostics with statement ID as payload", () => {
      const statementId = "some-id";
      const action = enqueueDiagnostics(statementId);
      return expectSaga(requestDiagnostics, action)
        .call(enqueueStatementDiagnostics, { statementId })
        .put(completeEnqueueDiagnostics())
        .dispatch(action)
        .run();
    });
  });
});
