// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import { all, call, put, takeLatest } from "redux-saga/effects";

import { getDatabasesList } from "src/api";

import { maybeError } from "../../util";

import { actions } from "./databasesList.reducers";

export function* refreshDatabasesListSaga() {
  yield put(actions.request());
}

export function* requestDatabasesListSaga(): any {
  try {
    const result = yield call(getDatabasesList, moment.duration(10, "m"));
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(maybeError(e)));
  }
}

export function* databasesListSaga() {
  yield all([
    takeLatest(actions.refresh, refreshDatabasesListSaga),
    takeLatest(actions.request, requestDatabasesListSaga),
  ]);
}
