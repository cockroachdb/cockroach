// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  TimeScale,
  api as clusterApi,
  api as clusterUiApi,
} from "@cockroachlabs/cluster-ui";
import {
  all,
  call,
  delay,
  put,
  takeEvery,
  takeLatest,
} from "redux-saga/effects";

import { PayloadAction, WithRequest } from "src/interfaces/action";
import {
  createStatementDiagnosticsAlertLocalSetting,
  cancelStatementDiagnosticsAlertLocalSetting,
} from "src/redux/alerts";
import {
  invalidateStatementDiagnosticsRequests,
  RECEIVE_STATEMENT_DIAGNOSTICS_REPORT,
  refreshStatementDiagnosticsRequests,
  statementDiagnosticInvalidationPeriod,
} from "src/redux/apiReducers";
import { setTimeScale } from "src/redux/timeScale";

import {
  CREATE_STATEMENT_DIAGNOSTICS_REPORT,
  createStatementDiagnosticsReportCompleteAction,
  createStatementDiagnosticsReportFailedAction,
  SET_GLOBAL_TIME_SCALE,
  CANCEL_STATEMENT_DIAGNOSTICS_REPORT,
  cancelStatementDiagnosticsReportCompleteAction,
  cancelStatementDiagnosticsReportFailedAction,
} from "./statementsActions";

export function* createDiagnosticsReportSaga(
  action: PayloadAction<clusterUiApi.InsertStmtDiagnosticRequest>,
) {
  try {
    yield call(clusterUiApi.createStatementDiagnosticsReport, action.payload);
    yield put(createStatementDiagnosticsReportCompleteAction());
    yield put(invalidateStatementDiagnosticsRequests());
    // PUT expects action with `type` field which isn't defined in `refresh` ThunkAction interface
    yield put(refreshStatementDiagnosticsRequests() as any);
    // Stop showing the "cancel statement" alert if it is currently showing
    // (i.e. accidental cancel, then immediate activate).
    yield put(
      cancelStatementDiagnosticsAlertLocalSetting.set({
        show: false,
      }),
    );
    yield put(
      createStatementDiagnosticsAlertLocalSetting.set({
        show: true,
        status: "SUCCESS",
      }),
    );
  } catch (e) {
    yield put(createStatementDiagnosticsReportFailedAction());
    // Stop showing the "cancel statement" alert if it is currently showing
    // (i.e. accidental cancel, then immediate activate).
    yield put(
      cancelStatementDiagnosticsAlertLocalSetting.set({
        show: false,
      }),
    );
    yield put(
      createStatementDiagnosticsAlertLocalSetting.set({
        show: true,
        status: "FAILED",
      }),
    );
  }
}

export function* cancelDiagnosticsReportSaga(
  action: PayloadAction<clusterUiApi.CancelStmtDiagnosticRequest>,
) {
  try {
    yield call(clusterUiApi.cancelStatementDiagnosticsReport, action.payload);

    yield put(cancelStatementDiagnosticsReportCompleteAction());

    yield put(invalidateStatementDiagnosticsRequests());
    // PUT expects action with `type` field which isn't defined in `refresh` ThunkAction interface.
    yield put(refreshStatementDiagnosticsRequests() as any);

    // Stop showing the "create statement" alert if it is currently showing
    // (i.e. accidental activate, then immediate cancel).
    yield put(
      createStatementDiagnosticsAlertLocalSetting.set({
        show: false,
      }),
    );
    yield put(
      cancelStatementDiagnosticsAlertLocalSetting.set({
        show: true,
        status: "SUCCESS",
      }),
    );
  } catch (e) {
    yield put(cancelStatementDiagnosticsReportFailedAction());
    // Stop showing the "create statement" alert if it is currently showing
    // (i.e. accidental activate, then immediate cancel).
    yield put(
      createStatementDiagnosticsAlertLocalSetting.set({
        show: false,
      }),
    );
    yield put(
      cancelStatementDiagnosticsAlertLocalSetting.set({
        show: true,
        status: "FAILED",
      }),
    );
  }
}

// receivedStatementDiagnosticsSaga creates a saga that handles RECEIVE action for statement diagnostics in
// addition to default workflow defined in CachedDataReducer.
// The main goal of this saga is request statement diagnostics results more often if received list of requested
// diagnostics has some diagnostics that is still not completed (with waiting status).
// If there's not completed request, we poll data every 30 seconds (or as fallback with default invalidation period
// that should be less than 30 seconds.
export const receivedStatementDiagnosticsSaga = (pollingDelay = 30000) => {
  const invalidationPeriodMs =
    statementDiagnosticInvalidationPeriod.asMilliseconds();
  const frequentPollingDelay = Math.min(invalidationPeriodMs, pollingDelay);
  return function* (
    action: PayloadAction<
      WithRequest<clusterApi.StatementDiagnosticsResponse, unknown>
    >,
  ) {
    // If we have active requests for statement diagnostics then poll for new data more often (every 30s or as defined
    // invalidation period (if it less than 30s).
    const hasActiveRequests = action.payload.data.some(s => !s.completed);
    if (!hasActiveRequests) {
      return;
    }
    yield delay(frequentPollingDelay);
    yield put(invalidateStatementDiagnosticsRequests());
    yield call(refreshStatementDiagnosticsRequests);
  };
};

export function* setCombinedStatementsTimeScaleSaga(
  action: PayloadAction<TimeScale>,
) {
  const ts = action.payload;

  yield put(setTimeScale(ts));
}

export function* statementsSaga() {
  yield all([
    takeEvery(CREATE_STATEMENT_DIAGNOSTICS_REPORT, createDiagnosticsReportSaga),
    takeEvery(CANCEL_STATEMENT_DIAGNOSTICS_REPORT, cancelDiagnosticsReportSaga),
    takeLatest(SET_GLOBAL_TIME_SCALE, setCombinedStatementsTimeScaleSaga),
    takeEvery(
      RECEIVE_STATEMENT_DIAGNOSTICS_REPORT,
      receivedStatementDiagnosticsSaga(),
    ),
  ]);
}
