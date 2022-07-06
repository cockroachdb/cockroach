// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, delay, put, takeLatest } from "redux-saga/effects";

import { actions } from "./contention.reducer";
import { getContentionTransactions } from "src/api/contentionApi";
import { throttleWithReset } from "../utils";
import { rootActions } from "../reducers";

export function* refreshContentionTransactionsSaga() {
  yield put(actions.request());
}

export function* requestContentionTransactionsSaga(): any {
  try {
    const result = yield call(getContentionTransactions);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedContentionTransactionsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* contentionEventsSaga(
  cacheInvalidationPeriod: number = 10 * 1000,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshContentionTransactionsSaga,
    ),
    takeLatest(actions.request, requestContentionTransactionsSaga),
    takeLatest(
      actions.received,
      receivedContentionTransactionsSaga,
      cacheInvalidationPeriod,
    ),
  ]);
}
