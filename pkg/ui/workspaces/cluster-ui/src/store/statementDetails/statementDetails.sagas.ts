// Copyright 2022 The Cockroach Authors.
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
import {
  ErrorWithKey,
  getStatementDetails,
  StatementDetailsRequest,
  StatementDetailsResponse,
  StatementDetailsResponseWithKey,
} from "src/api/statementsApi";
import { actions as sqlDetailsStatsActions } from "./statementDetails.reducer";
import { rootActions } from "../reducers";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";
import { generateStmtDetailsToID } from "../../statementDetails/statementDetails.selectors";

export function* refreshSQLDetailsStatsSaga(
  action?: PayloadAction<StatementDetailsRequest>,
) {
  yield put(sqlDetailsStatsActions.request(action?.payload));
}

export function* requestSQLDetailsStatsSaga(
  action?: PayloadAction<StatementDetailsRequest>,
): any {
  const key = generateStmtDetailsToID(
    action?.payload.fingerprint_id,
    action?.payload.app_names.toString(),
  );
  try {
    const result = yield call(getStatementDetails, action?.payload);
    const resultWithKey: StatementDetailsResponseWithKey = {
      stmtResponse: result,
      key,
    };
    yield put(sqlDetailsStatsActions.received(resultWithKey));
  } catch (e) {
    const err: ErrorWithKey = {
      err: e,
      key,
    };
    yield put(sqlDetailsStatsActions.failed(err));
  }
}

export function receivedSQLDetailsStatsSagaFactory(delayMs: number) {
  return function* receivedSQLDetailsStatsSaga(
    action: PayloadAction<StatementDetailsResponseWithKey>,
  ) {
    yield delay(delayMs);
    yield put(
      sqlDetailsStatsActions.invalidated({
        key: action?.payload.key,
      }),
    );
  };
}

export function* sqlDetailsStatsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      sqlDetailsStatsActions.refresh,
      [sqlDetailsStatsActions.invalidated, rootActions.resetState],
      refreshSQLDetailsStatsSaga,
    ),
    takeLatest(sqlDetailsStatsActions.request, requestSQLDetailsStatsSaga),
    takeLatest(
      sqlDetailsStatsActions.received,
      receivedSQLDetailsStatsSagaFactory(cacheInvalidationPeriod),
    ),
  ]);
}
