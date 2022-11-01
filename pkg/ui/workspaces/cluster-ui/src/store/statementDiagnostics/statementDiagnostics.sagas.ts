// Copyright 2021 The Cockroach Authors.
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
  call,
  delay,
  put,
  takeEvery,
  takeLatest,
} from "redux-saga/effects";
import {
  cancelStatementDiagnosticsReport,
  createStatementDiagnosticsReport,
  getStatementDiagnosticsReports,
} from "src/api/statementDiagnosticsApi";
import { actions } from "./statementDiagnostics.reducer";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "../utils";
import { rootActions } from "../reducers";

export function* createDiagnosticsReportSaga(
  action: ReturnType<typeof actions.createReport>,
) {
  try {
    yield call(createStatementDiagnosticsReport, action.payload);
    yield put(actions.createReportCompleted());
    // request diagnostics reports to reflect changed state for newly
    // requested statement.
    yield put(actions.request());
  } catch (e) {
    yield put(actions.createReportFailed(e));
  }
}

// TODO(#75559): We would like to alert on a resulting success/failure with this saga.
// However, the alerting component used on CC console lives in the managed
// service repo (Notification in notification.tsx) and cannot be used in a
// cluster-ui saga. Issue has been reported to merge the AlertBanner and
// Notification components from db-console & managed service respectively, to
// have a single component in cluster-ui to be used by both repos.
export function* cancelDiagnosticsReportSaga(
  action: ReturnType<typeof actions.cancelReport>,
) {
  try {
    yield call(cancelStatementDiagnosticsReport, action.payload);
    yield put(actions.cancelReportCompleted());
    yield put(actions.request());
  } catch (e) {
    yield put(actions.cancelReportFailed(e));
  }
}

export function* refreshStatementsDiagnosticsSaga() {
  yield put(actions.request());
}

export function* requestStatementsDiagnosticsSaga(): any {
  try {
    const response = yield call(getStatementDiagnosticsReports);
    yield put(actions.received(response));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedStatementsDiagnosticsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* statementsDiagnosticsSagas(
  delayMs: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      delayMs,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshStatementsDiagnosticsSaga,
    ),
    takeLatest(actions.request, requestStatementsDiagnosticsSaga),
    takeEvery(actions.createReport, createDiagnosticsReportSaga),
    takeEvery(actions.cancelReport, cancelDiagnosticsReportSaga),
    takeLatest(actions.received, receivedStatementsDiagnosticsSaga, delayMs),
  ]);
}
