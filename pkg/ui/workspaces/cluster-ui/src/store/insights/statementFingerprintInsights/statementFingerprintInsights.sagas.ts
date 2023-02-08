// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeLatest } from "redux-saga/effects";

import {
  actions,
  FingerprintInsightResponseWithKey,
} from "./statementFingerprintInsights.reducer";
import { PayloadAction } from "@reduxjs/toolkit";
import {
  ErrorWithKey,
  StmtInsightsReq,
  getStmtInsightsApi,
} from "../../../api";
import { HexStringToInt64String } from "../../../util";

export function* refreshStatementFingerprintInsightsSaga(
  action: PayloadAction<StmtInsightsReq>,
): any {
  yield put(actions.request(action.payload));
}

export function* requestStatementFingerprintInsightsSaga(
  action: PayloadAction<StmtInsightsReq>,
): any {
  const key = HexStringToInt64String(action.payload.stmtFingerprintId);
  try {
    const result = yield call(getStmtInsightsApi, action.payload);
    const resultWithKey: FingerprintInsightResponseWithKey = {
      response: result,
      key,
    };
    yield put(actions.received(resultWithKey));
  } catch (e) {
    const err: ErrorWithKey = {
      err: e,
      key: action.payload.stmtFingerprintId,
    };
    yield put(actions.failed(err));
  }
}

const CACHE_INVALIDATION_PERIOD = 5 * 60 * 1000; // 5 minutes in ms

const timeoutsByFingerprintID = new Map<string, NodeJS.Timeout>();

export function receivedStatementFingerprintInsightsSaga(
  action: PayloadAction<FingerprintInsightResponseWithKey>,
) {
  const fingerprintID = action.payload.key;
  clearTimeout(timeoutsByFingerprintID.get(fingerprintID));
  const id = setTimeout(() => {
    actions.invalidated({ key: fingerprintID });
    timeoutsByFingerprintID.delete(fingerprintID);
  }, CACHE_INVALIDATION_PERIOD);
  timeoutsByFingerprintID.set(fingerprintID, id);
}

export function* statementFingerprintInsightsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshStatementFingerprintInsightsSaga),
    takeLatest(actions.request, requestStatementFingerprintInsightsSaga),
    takeLatest(actions.received, receivedStatementFingerprintInsightsSaga),
  ]);
}
