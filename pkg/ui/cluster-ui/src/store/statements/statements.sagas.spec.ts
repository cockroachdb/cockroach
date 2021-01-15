import { expectSaga } from "redux-saga-test-plan";
import { throwError } from "redux-saga-test-plan/providers";
import * as matchers from "redux-saga-test-plan/matchers";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { getStatements } from "src/api/statementsApi";
import {
  receivedStatementsSaga,
  refreshStatementsSaga,
  requestStatementsSaga,
} from "./statements.sagas";
import { actions, reducer, StatementsState } from "./statements.reducer";

describe("StatementsPage sagas", () => {
  const statements = new cockroach.server.serverpb.StatementsResponse({
    statements: [],
    last_reset: null,
  });

  describe("refreshStatementsSaga", () => {
    it("dispatches request statements action", () => {
      expectSaga(refreshStatementsSaga)
        .put(actions.request())
        .run();
    });
  });

  describe("requestStatementsSaga", () => {
    it("successfully requests statements list", () => {
      expectSaga(requestStatementsSaga)
        .provide([[matchers.call.fn(getStatements), statements]])
        .put(actions.received(statements))
        .withReducer(reducer)
        .hasFinalState<StatementsState>({
          data: statements,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      expectSaga(requestStatementsSaga)
        .provide([[matchers.call.fn(getStatements), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<StatementsState>({
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
      expectSaga(receivedStatementsSaga, timeout)
        .delay(timeout)
        .put(actions.invalidated())
        .withReducer(reducer, {
          data: statements,
          lastError: null,
          valid: true,
        })
        .hasFinalState<StatementsState>({
          data: statements,
          lastError: null,
          valid: false,
        })
        .run(1000);
    });
  });
});
