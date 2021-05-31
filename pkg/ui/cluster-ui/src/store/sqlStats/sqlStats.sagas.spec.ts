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

import { resetSQLStatsSaga } from "./sqlStats.sagas";
import { resetSQLStats } from "../../api/sqlStatsApi";
import {
  actions as sqlStatsActions,
  reducer as sqlStatsReducers,
  ResetSQLStatsState,
} from "./sqlStats.reducer";
import { actions as statementActions } from "src/store/statements/statements.reducer";

describe("SQL Stats sagas", () => {
  describe("resetSQLStatsSaga", () => {
    const resetSQLStatsResponse = new cockroach.server.serverpb.ResetSQLStatsResponse();

    it("successfully resets SQL stats", () => {
      expectSaga(resetSQLStatsSaga)
        .provide([[matchers.call.fn(resetSQLStats), resetSQLStatsResponse]])
        .put(sqlStatsActions.received(resetSQLStatsResponse))
        .put(statementActions.invalidated())
        .put(statementActions.refresh())
        .withReducer(sqlStatsReducers)
        .hasFinalState<ResetSQLStatsState>({
          data: resetSQLStatsResponse,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("returns error on failed reset", () => {
      const err = new Error("failed to reset");
      expectSaga(resetSQLStatsSaga)
        .provide([[matchers.call.fn(resetSQLStats), throwError(err)]])
        .put(sqlStatsActions.failed(err))
        .withReducer(sqlStatsReducers)
        .hasFinalState<ResetSQLStatsState>({
          data: null,
          lastError: err,
          valid: false,
        })
        .run();
    });
  });
});
