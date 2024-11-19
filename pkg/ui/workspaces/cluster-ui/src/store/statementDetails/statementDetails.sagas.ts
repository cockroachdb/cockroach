// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeLatest } from "redux-saga/effects";

import {
  ErrorWithKey,
  getStatementDetails,
  StatementDetailsRequest,
  StatementDetailsResponseWithKey,
} from "src/api/statementsApi";
import { generateStmtDetailsToID } from "src/util/appStats";

import { maybeError } from "../../util";

import { actions as sqlDetailsStatsActions } from "./statementDetails.reducer";

export function* refreshSQLDetailsStatsSaga(
  action: PayloadAction<StatementDetailsRequest>,
) {
  yield put(sqlDetailsStatsActions.request(action?.payload));
}

export function* requestSQLDetailsStatsSaga(
  action: PayloadAction<StatementDetailsRequest>,
): any {
  const key = action?.payload
    ? generateStmtDetailsToID(
        action.payload.fingerprint_id,
        action.payload.app_names.toString(),
        action.payload.start,
        action.payload.end,
      )
    : "";
  try {
    const result = yield call(getStatementDetails, action?.payload);
    const resultWithKey: StatementDetailsResponseWithKey = {
      stmtResponse: result,
      key,
    };
    yield put(sqlDetailsStatsActions.received(resultWithKey));
  } catch (e) {
    const err: ErrorWithKey = {
      err: maybeError(e),
      key,
    };
    yield put(sqlDetailsStatsActions.failed(err));
  }
}

export function* sqlDetailsStatsSaga() {
  yield all([
    takeLatest(sqlDetailsStatsActions.refresh, refreshSQLDetailsStatsSaga),
    takeLatest(sqlDetailsStatsActions.request, requestSQLDetailsStatsSaga),
  ]);
}
