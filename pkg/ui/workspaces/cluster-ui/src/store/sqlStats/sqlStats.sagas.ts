// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, put, takeLatest } from "redux-saga/effects";

import { actions as localStorageActions } from "src/store/localStorage";

import {
  actions as sqlStatsActions,
  UpdateTimeScalePayload,
} from "./sqlStats.reducer";

export function* updateSQLStatsTimeScaleSaga(
  action: PayloadAction<UpdateTimeScalePayload>,
) {
  const { ts } = action.payload;
  yield put(
    localStorageActions.updateTimeScale({
      value: ts,
    }),
  );
}

export function* sqlStatsSaga() {
  yield all([
    takeLatest(sqlStatsActions.updateTimeScale, updateSQLStatsTimeScaleSaga),
  ]);
}
