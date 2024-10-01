// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
import { maybeError } from "src/util";

import { rootActions } from "../rootActions";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "../utils";

import { actions } from "./statementDiagnostics.reducer";

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
    yield put(actions.createReportFailed(maybeError(e)));
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
    yield put(actions.cancelReportFailed(maybeError(e)));
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
    yield put(actions.failed(maybeError(e)));
  }
}

export const receivedStatementsDiagnosticsSaga = (delayMs: number) => {
  const frequentPollingDelay = Math.min(delayMs, 30000);
  return function* (action: ReturnType<typeof actions.received>) {
    // If we have active requests for statement diagnostics then poll for new data more often (every 30s or as defined
    // with CACHE_INVALIDATION_PERIOD (if it less than 30s).
    const hasActiveRequests = action.payload.some(s => !s.completed);
    yield delay(hasActiveRequests ? frequentPollingDelay : delayMs);
    yield put(actions.invalidated());
  };
};

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
    takeLatest(actions.received, receivedStatementsDiagnosticsSaga(delayMs)),
  ]);
}
