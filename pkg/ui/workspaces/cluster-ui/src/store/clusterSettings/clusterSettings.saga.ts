// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { all, call, put, takeLatest } from "redux-saga/effects";

import { actions } from "./clusterSettings.reducer";
import { PayloadAction } from "@reduxjs/toolkit";
import {
  getClusterSettings,
  SettingsRequestMessage,
} from "../../api/clusterSettingsApi";

export function* refreshClusterSettingsSaga(
  action: PayloadAction<SettingsRequestMessage>,
) {
  yield put(actions.request(action.payload));
}

export function* requestClusterSettingsSaga(
  action: PayloadAction<SettingsRequestMessage>,
): any {
  try {
    const result = yield call(getClusterSettings, action.payload, "1M");
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* clusterSettingsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshClusterSettingsSaga),
    takeLatest(actions.request, requestClusterSettingsSaga),
  ]);
}
