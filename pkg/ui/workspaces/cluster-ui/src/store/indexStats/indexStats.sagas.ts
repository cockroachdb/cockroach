// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
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
import { CACHE_INVALIDATION_PERIOD } from "src/store/utils";

import {
  getIndexStats,
  resetIndexStats,
  TableIndexStatsRequest,
  TableIndexStatsResponseWithKey,
} from "../../api/indexDetailsApi";
import { generateTableID, maybeError } from "../../util";

import {
  actions as indexStatsActions,
  ResetIndexUsageStatsPayload,
} from "./indexStats.reducer";

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
      err: maybeError(e),
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
  action: PayloadAction<ResetIndexUsageStatsPayload>,
) {
  const key = action?.payload
    ? generateTableID(action.payload.database, action.payload.table)
    : "";
  const resetIndexUsageStatsRequest =
    new cockroach.server.serverpb.ResetIndexUsageStatsRequest();
  try {
    yield call(resetIndexStats, resetIndexUsageStatsRequest);
    yield put(indexStatsActions.invalidateAll());
    yield put(
      indexStatsActions.refresh(
        new cockroach.server.serverpb.TableIndexStatsRequest({
          ...action.payload,
        }),
      ),
    );
  } catch (e) {
    const err: ErrorWithKey = {
      err: maybeError(e),
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
    takeEvery(indexStatsActions.reset, resetIndexStatsSaga),
  ]);
}
