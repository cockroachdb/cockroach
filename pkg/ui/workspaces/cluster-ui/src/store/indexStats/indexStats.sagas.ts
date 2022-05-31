// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "@reduxjs/toolkit";
import {
  all,
  call,
  put,
  delay,
  takeLatest,
  takeEvery,
} from "redux-saga/effects";
import { ErrorWithKey } from "src/api/statementsApi";
import { actions as indexStatsActions } from "./indexStats.reducer";
import { CACHE_INVALIDATION_PERIOD } from "src/store/utils";
import { generateTableID } from "../../util";
import {
  getIndexStats,
  resetIndexStats,
  TableIndexStatsRequest,
  TableIndexStatsResponseWithKey,
} from "../../api/indexDetailsApi";

export function* refreshIndexStatsSaga(
  action: PayloadAction<TableIndexStatsRequest>,
) {
  yield put(indexStatsActions.request(action?.payload));
}

export function* requestIndexStatsSaga(
  action: PayloadAction<TableIndexStatsRequest>,
): any {
  const key = action?.payload
    ? generateTableID(action.payload.database, action.payload.table)
    : "";
  try {
    const result = yield call(getIndexStats, action?.payload);
    const resultWithKey: TableIndexStatsResponseWithKey = {
      indexStatsResponse: result,
      key,
    };
    yield put(indexStatsActions.received(resultWithKey));
  } catch (e) {
    const err: ErrorWithKey = {
      err: e,
      key,
    };
    yield put(indexStatsActions.failed(err));
  }
}

export function receivedIndexStatsSagaFactory(delayMs: number) {
  return function* receivedIndexStatsSaga(
    action: PayloadAction<TableIndexStatsResponseWithKey>,
  ) {
    yield delay(delayMs);
    yield put(
      indexStatsActions.invalidated({
        key: action?.payload.key,
      }),
    );
  };
}

export function* resetIndexStatsSaga(
  action: PayloadAction<TableIndexStatsRequest>,
) {
  const key = action?.payload
    ? generateTableID(action.payload.database, action.payload.table)
    : "";
  try {
    yield call(resetIndexStats);
    yield put(indexStatsActions.invalidateAll());
    yield put(indexStatsActions.refresh(action.payload));
  } catch (e) {
    const err: ErrorWithKey = {
      err: e,
      key,
    };
    yield put(indexStatsActions.failed(err));
  }
}

export function* indexStatsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    takeLatest(indexStatsActions.refresh, refreshIndexStatsSaga),
    takeLatest(indexStatsActions.request, requestIndexStatsSaga),
    takeLatest(
      indexStatsActions.received,
      receivedIndexStatsSagaFactory(cacheInvalidationPeriod),
    ),
    takeEvery(indexStatsActions.reset, resetIndexStats),
  ]);
}
