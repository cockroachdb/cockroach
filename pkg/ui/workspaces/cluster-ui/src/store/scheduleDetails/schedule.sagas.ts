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

import { actions } from "./schedule.reducer";
import { getSchedule, ScheduleRequest } from "src/api/schedulesApi";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "../utils";
import { rootActions } from "../reducers";
import { PayloadAction } from "@reduxjs/toolkit";

export function* refreshScheduleSaga(action: PayloadAction<ScheduleRequest>) {
  yield put(actions.request(action.payload));
}

export function* requestScheduleSaga(
  action: PayloadAction<ScheduleRequest>,
): any {
  try {
    const result = yield call(getSchedule, action.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedScheduleSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* scheduleSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshScheduleSaga,
    ),
    takeLatest(actions.request, requestScheduleSaga),
    takeLatest(actions.received, receivedScheduleSaga, cacheInvalidationPeriod),
  ]);
}
