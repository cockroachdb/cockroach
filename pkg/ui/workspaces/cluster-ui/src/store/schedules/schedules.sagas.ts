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

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { actions } from "./schedules.reducer";
import { getSchedules, SchedulesRequest } from "src/api/schedulesApi";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "../utils";
import { PayloadAction } from "@reduxjs/toolkit";
import { rootActions } from "../reducers";

export function* refreshSchedulesSaga(action: PayloadAction<SchedulesRequest>) {
  yield put(actions.request(action.payload));
}

export function* requestSchedulesSaga(
  action: PayloadAction<SchedulesRequest>,
): any {
  try {
    const result = yield call(getSchedules, action.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedSchedulesSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* updateFilteredSchedulesSaga(
  action: PayloadAction<SchedulesRequest>,
) {
  yield put(actions.invalidated());
  const req = new cockroach.server.serverpb.SchedulesRequest(action.payload);
  yield put(actions.refresh(req));
}

export function* schedulesSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshSchedulesSaga,
    ),
    takeLatest(actions.request, requestSchedulesSaga),
    takeLatest(
      actions.received,
      receivedSchedulesSaga,
      cacheInvalidationPeriod,
    ),
    takeLatest(actions.updateFilteredSchedules, updateFilteredSchedulesSaga),
  ]);
}
