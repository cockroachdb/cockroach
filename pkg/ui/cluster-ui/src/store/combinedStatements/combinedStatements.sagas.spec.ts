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
import * as matchers from "redux-saga-test-plan/matchers";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { getCombinedStatements } from "src/api/statementsApi";
import {
  receivedCombinedStatementsSaga,
  refreshCombinedStatementsSaga,
  requestCombinedStatementsSaga,
} from "./combinedStatements.sagas";
import {
  actions,
  reducer,
  CombinedStatementsState,
} from "./combinedStatements.reducer";
import { selectDateRange } from "src/statementsPage/statementsPage.selectors";

describe("StatementsPage sagas", () => {
  const statements = new cockroach.server.serverpb.StatementsResponse({
    statements: [],
    last_reset: null,
  });

  describe("refreshStatementsSaga", () => {
    it("dispatches request statements action", () => {
      expectSaga(refreshCombinedStatementsSaga)
        .provide([[matchers.select(selectDateRange), { start: 0, end: 0 }]])
        .put(actions.request())
        .run();
    });
  });

  describe("requestStatementsSaga", () => {
    it("successfully requests statements list", () => {
      expectSaga(requestCombinedStatementsSaga)
        .provide([
          [matchers.select(selectDateRange), { start: 0, end: 0 }],
          [matchers.call.fn(getCombinedStatements), statements],
        ])
        .put(actions.received(statements))
        .withReducer(reducer)
        .hasFinalState<CombinedStatementsState>({
          data: statements,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      expectSaga(requestCombinedStatementsSaga)
        .provide([
          [matchers.select(selectDateRange), { start: 0, end: 0 }],
          [matchers.call.fn(getCombinedStatements), throwError(error)],
        ])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<CombinedStatementsState>({
          data: null,
          lastError: error,
          valid: false,
        })
        .run();
    });
  });

  describe("receivedStatementsSaga", () => {
    it("sets valid status to false after specified period of time", () => {
      const timeout = 500;
      expectSaga(receivedCombinedStatementsSaga, timeout)
        .delay(timeout)
        .put(actions.invalidated())
        .withReducer(reducer, {
          data: statements,
          lastError: null,
          valid: true,
        })
        .hasFinalState<CombinedStatementsState>({
          data: statements,
          lastError: null,
          valid: false,
        })
        .run(1000);
    });
  });
});
