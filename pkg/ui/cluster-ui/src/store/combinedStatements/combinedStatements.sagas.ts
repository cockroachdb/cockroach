// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, delay, takeLatest, select } from "redux-saga/effects";
import {
  getCombinedStatements,
  CombinedStatementsRequest,
} from "src/api/statementsApi";
import { actions } from "./combinedStatements.reducer";
import { rootActions } from "../reducers";
import { actions as localStorageActions } from "../localStorage";

import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";
import { PayloadAction } from "@reduxjs/toolkit";
import { selectDateRange } from "src/statementsPage/statementsPage.selectors";

export function* refreshCombinedStatementsSaga() {
  yield put(actions.request());
}

export function* requestCombinedStatementsSaga() {
  try {
    const req = yield select(selectDateRange);
    const result = yield call(getCombinedStatements, req);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedCombinedStatementsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* updateStatementesDateRangeSaga(
  action: PayloadAction<Required<CombinedStatementsRequest>>,
) {
  yield put(
    localStorageActions.update({
      key: "dateRange/StatementsPage",
      value: {
        start: action.payload.start,
        end: action.payload.end,
      },
    }),
  );
  yield put(actions.refresh());
}

export function* combinedStatementsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, actions.failed, rootActions.resetState],
      refreshCombinedStatementsSaga,
    ),
    takeLatest(actions.request, requestCombinedStatementsSaga),
    takeLatest(actions.updateDateRange, updateStatementesDateRangeSaga),
    takeLatest(
      actions.received,
      receivedCombinedStatementsSaga,
      cacheInvalidationPeriod,
    ),
  ]);
}
