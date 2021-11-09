// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, delay, takeLatest } from "redux-saga/effects";
import Long from "long";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import {
  getStatements,
  getCombinedStatements,
  StatementsRequest,
} from "src/api/statementsApi";
import { actions as localStorageActions } from "src/store/localStorage";
import {
  actions as statementActions,
  UpdateDateRangePayload,
} from "./statements.reducer";
import { actions as transactionActions } from "src/store/transactions";
import { rootActions } from "../reducers";

import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";

export function* refreshStatementsSaga(
  action?: PayloadAction<StatementsRequest>,
) {
  yield put(statementActions.request(action?.payload));
}

export function* requestStatementsSaga(
  action?: PayloadAction<StatementsRequest>,
): any {
  try {
    const result = yield action?.payload?.combined
      ? call(getCombinedStatements, action.payload)
      : call(getStatements);
    yield put(statementActions.received(result));
    yield put(transactionActions.received(result));
  } catch (e) {
    yield put(statementActions.failed(e));
  }
}

export function* receivedStatementsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(statementActions.invalidated());
}

export function* updateStatementesDateRangeSaga(
  action: PayloadAction<UpdateDateRangePayload>,
) {
  const { start, end } = action.payload;
  yield put(
    localStorageActions.update({
      key: "dateRange/StatementsPage",
      value: { start, end },
    }),
  );
  yield put(statementActions.invalidated());
  const req = new cockroach.server.serverpb.StatementsRequest({
    combined: true,
    start: Long.fromNumber(start),
    end: Long.fromNumber(end),
  });
  yield put(statementActions.refresh(req));
}

export function* statementsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      statementActions.refresh,
      [
        statementActions.invalidated,
        statementActions.failed,
        rootActions.resetState,
      ],
      refreshStatementsSaga,
    ),
    takeLatest(statementActions.request, requestStatementsSaga),
    takeLatest(
      statementActions.received,
      receivedStatementsSaga,
      cacheInvalidationPeriod,
    ),
    takeLatest(
      statementActions.updateDateRange,
      updateStatementesDateRangeSaga,
    ),
  ]);
}
