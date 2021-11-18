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
  resetIndexUsageStatsFailedAction,
  resetIndexUsageStatsCompleteAction,
  resetIndexUsageStatsAction,
} from "./indexUsageStatsActions";
import { resetIndexUsageStatsSaga } from "./indexUsageStatsSagas";
import { resetIndexUsageStats } from "src/util/api";
import { refreshIndexStats } from "src/redux/apiReducers";
import { throwError } from "redux-saga-test-plan/providers";

import { cockroach } from "src/js/protos";


describe("Index Usage Stats sagas", () => {
  describe("resetIndexUsageStatsSaga", () => {
    const resetIndexUsageStatsResponse = new cockroach.server.serverpb.ResetIndexUsageStatsResponse();
    const action = resetIndexUsageStatsAction("database", "table");

    it("successfully resets index usage stats", () => {
      expectSaga(resetIndexUsageStatsSaga, action)
        .provide([
          [call.fn(resetIndexUsageStats), resetIndexUsageStatsResponse],
        ])
        .put(resetIndexUsageStatsCompleteAction())
        .put(refreshIndexStats() as any)
        .run();
    });

    it("returns error on failed reset", () => {
      const err = new Error("failed to reset");
      expectSaga(resetIndexUsageStatsSaga, action)
        .provide([[call.fn(resetIndexUsageStats), throwError(err)]])
        .put(resetIndexUsageStatsFailedAction())
        .run();
    });
  });
});
