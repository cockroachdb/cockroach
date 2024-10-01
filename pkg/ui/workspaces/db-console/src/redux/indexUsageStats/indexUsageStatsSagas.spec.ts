// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { expectSaga } from "redux-saga-test-plan";
import { call, select } from "redux-saga-test-plan/matchers";
import { throwError } from "redux-saga-test-plan/providers";

import { cockroach } from "src/js/protos";
import { resetIndexUsageStats } from "src/util/api";

import {
  resetIndexUsageStatsFailedAction,
  resetIndexUsageStatsCompleteAction,
  resetIndexUsageStatsAction,
} from "./indexUsageStatsActions";
import {
  resetIndexUsageStatsSaga,
  selectIndexStatsKeys,
} from "./indexUsageStatsSagas";

describe("Index Usage Stats sagas", () => {
  describe("resetIndexUsageStatsSaga", () => {
    const resetIndexUsageStatsResponse =
      new cockroach.server.serverpb.ResetIndexUsageStatsResponse();
    const action = resetIndexUsageStatsAction("database", "table");

    it("successfully resets index usage stats", () => {
      // TODO(lindseyjin): figure out how to test invalidate and refresh actions
      //  once we can figure out how to get ThunkAction to work with sagas.
      return expectSaga(resetIndexUsageStatsSaga, action)
        .provide([
          [call.fn(resetIndexUsageStats), resetIndexUsageStatsResponse],
          [select(selectIndexStatsKeys), ["database/table"]],
        ])
        .put(resetIndexUsageStatsCompleteAction())
        .dispatch(action)
        .run();
    });

    it("returns error on failed reset", () => {
      const err = new Error("failed to reset");
      return expectSaga(resetIndexUsageStatsSaga, action)
        .provide([[call.fn(resetIndexUsageStats), throwError(err)]])
        .put(resetIndexUsageStatsFailedAction())
        .dispatch(action)
        .run();
    });
  });
});
