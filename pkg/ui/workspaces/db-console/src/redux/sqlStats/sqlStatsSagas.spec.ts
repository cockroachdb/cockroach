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
import { call } from "redux-saga-test-plan/matchers";

import {
  resetSQLStatsFailedAction,
  resetSQLStatsCompleteAction,
  resetSQLStatsAction,
} from "./sqlStatsActions";
import { resetSQLStatsSaga } from "./sqlStatsSagas";
import { resetSQLStats } from "src/util/api";
import {
  apiReducersReducer,
  invalidateStatements,
  invalidateAllStatementDetails,
} from "src/redux/apiReducers";
import { throwError } from "redux-saga-test-plan/providers";

import { cockroach } from "src/js/protos";
import Long from "long";

describe("SQL Stats sagas", () => {
  describe("resetSQLStatsSaga", () => {
    const payload = new cockroach.server.serverpb.StatementsRequest({
      start: Long.fromNumber(1596816675),
      end: Long.fromNumber(1596820675),
      combined: false,
    });
    const resetSQLStatsResponse = new cockroach.server.serverpb.ResetSQLStatsResponse();

    it("successfully resets SQL stats", () => {
      // TODO(azhng): validate refreshStatement() actions once we can figure out
      //  how to get ThunkAction to work with sagas.
      return expectSaga(resetSQLStatsSaga, resetSQLStatsAction(payload))
        .withReducer(apiReducersReducer)
        .provide([[call.fn(resetSQLStats), resetSQLStatsResponse]])
        .put(resetSQLStatsCompleteAction())
        .put(invalidateStatements())
        .put(invalidateAllStatementDetails())
        .run();
    });

    it("returns error on failed reset", () => {
      const err = new Error("failed to reset");
      return expectSaga(resetSQLStatsSaga, resetSQLStatsAction(payload))
        .provide([[call.fn(resetSQLStats), throwError(err)]])
        .put(resetSQLStatsFailedAction())
        .run();
    });
  });
});
