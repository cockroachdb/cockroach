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

import { getStatements } from "src/api/statementsApi";

import {
  receivedTransactionsSaga,
  refreshTransactionsSaga,
  requestTransactionsSaga,
} from "./transactions.sagas";
import { actions, reducer, TransactionsState } from "./transactions.reducer";

describe("TransactionsPage sagas", () => {
  const statements = new cockroach.server.serverpb.StatementsResponse({
    statements: [],
    last_reset: null,
  });

  describe("refreshTransactionsSaga", () => {
    it("dispatches request transactions action", () => {
      expectSaga(refreshTransactionsSaga)
        .put(actions.request())
        .run();
    });
  });

  describe("requestStatementsSaga", () => {
    it("successfully requests statements list", () => {
      expectSaga(requestTransactionsSaga)
        .provide([[matchers.call.fn(getStatements), statements]])
        .put(actions.received(statements))
        .withReducer(reducer)
        .hasFinalState<TransactionsState>({
          data: statements,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      expectSaga(requestTransactionsSaga)
        .provide([[matchers.call.fn(getStatements), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<TransactionsState>({
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
      expectSaga(receivedTransactionsSaga, timeout)
        .delay(timeout)
        .put(actions.invalidated())
        .withReducer(reducer, {
          data: statements,
          lastError: null,
          valid: true,
        })
        .hasFinalState<TransactionsState>({
          data: statements,
          lastError: null,
          valid: false,
        })
        .run(1000);
    });
  });
});
