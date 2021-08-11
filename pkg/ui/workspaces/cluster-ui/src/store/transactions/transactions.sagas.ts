// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, delay, takeLatest } from "redux-saga/effects";
import {
  getStatements,
  getCombinedStatements,
  StatementsRequest,
} from "src/api/statementsApi";
import { actions } from "./transactions.reducer";
import { rootActions } from "../reducers";

import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";

export function* refreshTransactionsSaga(
  action?: PayloadAction<StatementsRequest>,
) {
  yield put(actions.request(action?.payload));
}

export function* requestTransactionsSaga(
  action?: PayloadAction<StatementsRequest>,
): any {
  try {
    const result = yield action?.payload?.combined
      ? call(getCombinedStatements, action.payload)
      : call(getStatements);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedTransactionsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* transactionsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, actions.failed, rootActions.resetState],
      refreshTransactionsSaga,
    ),
    takeLatest(actions.request, requestTransactionsSaga),
    takeLatest(
      actions.received,
      receivedTransactionsSaga,
      cacheInvalidationPeriod,
    ),
  ]);
}
