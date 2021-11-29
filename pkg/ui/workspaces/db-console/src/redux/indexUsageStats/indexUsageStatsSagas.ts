// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "src/js/protos";
import { all, call, put, takeEvery, select } from "redux-saga/effects";
import {
  RESET_INDEX_USAGE_STATS,
  resetIndexUsageStatsCompleteAction,
  resetIndexUsageStatsFailedAction,
  resetIndexUsageStatsPayload,
} from "./indexUsageStatsActions";

import ResetIndexUsageStatsRequest = cockroach.server.serverpb.ResetIndexUsageStatsRequest;
import {
  invalidateIndexStats,
  KeyedCachedDataReducerState,
  refreshIndexStats,
} from "src/redux/apiReducers";
import { IndexStatsResponseMessage, resetIndexUsageStats } from "src/util/api";
import { createSelector } from "reselect";
import { AdminUIState } from "src/redux/state";
import TableIndexStatsRequest = cockroach.server.serverpb.TableIndexStatsRequest;
import { PayloadAction } from "src/interfaces/action";

export const selectIndexStatsKeys = createSelector(
  (state: AdminUIState) => state.cachedData.indexStats,
  (indexUsageStats: KeyedCachedDataReducerState<IndexStatsResponseMessage>) =>
    Object.keys(indexUsageStats),
);

export const KeyToTableRequest = (key: string): TableIndexStatsRequest => {
  const s = key.split("/");
  const database = s[0];
  const table = s[1];
  return new TableIndexStatsRequest({ database, table });
};
export function* resetIndexUsageStatsSaga(
  action: PayloadAction<resetIndexUsageStatsPayload>,
) {
  const resetIndexUsageStatsRequest = new ResetIndexUsageStatsRequest();
  const { database, table } = action.payload;
  try {
    yield call(resetIndexUsageStats, resetIndexUsageStatsRequest);
    yield put(resetIndexUsageStatsCompleteAction());

    // invalidate all index stats in cache.
    const keys: string[] = yield select(selectIndexStatsKeys);
    yield keys.forEach(key =>
      put(invalidateIndexStats(KeyToTableRequest(key))),
    );

    // refresh index stats for table page that user is on.
    yield put(
      refreshIndexStats(new TableIndexStatsRequest({ database, table })) as any,
    );
  } catch (e) {
    yield put(resetIndexUsageStatsFailedAction());
  }
}

export function* indexUsageStatsSaga() {
  yield all([takeEvery(RESET_INDEX_USAGE_STATS, resetIndexUsageStatsSaga)]);
}
