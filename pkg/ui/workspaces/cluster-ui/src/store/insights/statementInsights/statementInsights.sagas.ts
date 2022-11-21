// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeLatest } from "redux-saga/effects";

import { actions } from "./statementInsights.reducer";
import {
  ExecutionInsightsRequest,
  getClusterInsightsApi,
  getTxnInsightEvents,
} from "src/api/insightsApi";
import { PayloadAction } from "@reduxjs/toolkit";

export function* refreshStatementInsightsSaga(
  action?: PayloadAction<ExecutionInsightsRequest>,
) {
  yield put(actions.request(action.payload));
}

export function* requestStatementInsightsSaga(
  action?: PayloadAction<ExecutionInsightsRequest>,
): any {
  try {
    let result: any;
    if (action) {
      result = yield call(getClusterInsightsApi, action.payload);
    } else {
      result = yield call(getClusterInsightsApi);
    }
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* statementInsightsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshStatementInsightsSaga),
    takeLatest(actions.request, requestStatementInsightsSaga),
  ]);
}
