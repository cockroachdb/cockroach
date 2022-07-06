// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, delay, put, takeLatest } from "redux-saga/effects";

import { actions } from "./insightDetails.reducer";
import {
  getInsightEventDetailsState,
  InsightEventDetailsRequest,
} from "src/api/insightsApi";
import { throttleWithReset } from "../utils";
import { rootActions } from "../reducers";
import { PayloadAction } from "@reduxjs/toolkit";

export function* refreshInsightDetailsSaga(
  action: PayloadAction<InsightEventDetailsRequest>,
) {
  yield put(actions.request(action.payload));
}

export function* requestInsightDetailsSaga(
  action: PayloadAction<InsightEventDetailsRequest>,
): any {
  try {
    const result = yield call(getInsightEventDetailsState, action.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* insightDetailsSaga() {
  yield all([takeLatest(actions.request, requestInsightDetailsSaga)]);
}
