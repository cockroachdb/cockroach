// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeLatest } from "redux-saga/effects";
import { PayloadAction } from "@reduxjs/toolkit";

import { SchemaInsightReqParams, getSchemaInsights } from "../../api";
import { maybeError } from "../../util";

import { actions } from "./schemaInsights.reducer";

export function* refreshSchemaInsightsSaga(
  action: PayloadAction<SchemaInsightReqParams>,
) {
  yield put(actions.request(action.payload));
}

export function* requestSchemaInsightsSaga(
  action: PayloadAction<SchemaInsightReqParams>,
): any {
  try {
    const result = yield call(getSchemaInsights, action.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(maybeError(e)));
  }
}

export function* schemaInsightsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshSchemaInsightsSaga),
    takeLatest(actions.request, requestSchemaInsightsSaga),
  ]);
}
