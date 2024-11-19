// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeLatest } from "redux-saga/effects";

import {
  getClusterSettings,
  SettingsRequestMessage,
} from "../../api/clusterSettingsApi";
import { maybeError } from "../../util";

import { actions } from "./clusterSettings.reducer";

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
    yield put(actions.failed(maybeError(e)));
  }
}

export function* clusterSettingsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshClusterSettingsSaga),
    takeLatest(actions.request, requestClusterSettingsSaga),
  ]);
}
