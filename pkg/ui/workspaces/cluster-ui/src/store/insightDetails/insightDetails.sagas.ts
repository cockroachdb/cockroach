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

import { actions } from "./insightDetails.reducer";
import {
  getTransactionInsightEventDetailsState,
  TransactionInsightEventDetailsRequest,
} from "src/api/insightsApi";
import { PayloadAction } from "@reduxjs/toolkit";

export function* refreshTransactionInsightDetailsSaga(
  action: PayloadAction<TransactionInsightEventDetailsRequest>,
) {
  yield put(actions.request(action.payload));
}

export function* requestTransactionInsightDetailsSaga(
  action: PayloadAction<TransactionInsightEventDetailsRequest>,
): any {
  try {
    const result = yield call(
      getTransactionInsightEventDetailsState,
      action.payload,
    );
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* transactionInsightDetailsSaga() {
  yield all([
    takeLatest(actions.request, requestTransactionInsightDetailsSaga),
  ]);
}
