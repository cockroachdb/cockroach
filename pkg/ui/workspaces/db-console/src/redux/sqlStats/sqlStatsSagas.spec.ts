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
} from "./sqlStatsActions";
import { resetSQLStatsSaga } from "./sqlStatsSagas";
import { resetSQLStats } from "src/util/api";
import { invalidateStatements, refreshStatements } from "src/redux/apiReducers";
import { throwError } from "redux-saga-test-plan/providers";

import { cockroach } from "src/js/protos";

describe("SQL Stats sagas", () => {
  describe("resetSQLStatsSaga", () => {
    const resetSQLStatsResponse = new cockroach.server.serverpb.ResetSQLStatsResponse();

    it("successfully resets SQL stats", () => {
      expectSaga(resetSQLStatsSaga)
        .provide([[call.fn(resetSQLStats), resetSQLStatsResponse]])
        .put(resetSQLStatsCompleteAction())
        .put(invalidateStatements())
        .put(refreshStatements() as any)
        .run();
    });

    it("returns error on failed reset", () => {
      const err = new Error("failed to reset");
      expectSaga(resetSQLStatsSaga)
        .provide([[call.fn(resetSQLStats), throwError(err)]])
        .put(resetSQLStatsFailedAction())
        .run();
    });
  });
});
