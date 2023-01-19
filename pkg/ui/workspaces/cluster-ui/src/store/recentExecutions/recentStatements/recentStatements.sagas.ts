// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  all,
  AllEffect,
  call,
  ForkEffect,
  put, takeLatest,
} from "redux-saga/effects";

import { actions } from "./recentStatements.reducer";
import { getRecentStatements, RecentStatementsRequestKey } from "src/api/recentExecutionsApi";
import { PayloadAction } from "@reduxjs/toolkit";

export function* refreshRecentStatementsSaga() {
  yield put(actions.request());
}

export function* requestRecentStatementsSaga(
  action: PayloadAction<RecentStatementsRequestKey>
): any {
  try {
    const result = yield call(
      getRecentStatements,
      action.payload,
    );
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* recentStatementsSaga(): Generator<AllEffect<ForkEffect>> {
  yield all([
    takeLatest(actions.request, requestRecentStatementsSaga),
    takeLatest(actions.refresh, refreshRecentStatementsSaga),
  ]);
}
