// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
