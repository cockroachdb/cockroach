// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeEvery, takeLatest } from "redux-saga/effects";
import { PayloadAction } from "src/interfaces/action";

import {
  CREATE_STATEMENT_DIAGNOSTICS_REPORT,
  createStatementDiagnosticsReportCompleteAction,
  createStatementDiagnosticsReportFailedAction,
  SET_GLOBAL_TIME_SCALE,
  CANCEL_STATEMENT_DIAGNOSTICS_REPORT,
  cancelStatementDiagnosticsReportCompleteAction,
  cancelStatementDiagnosticsReportFailedAction,
} from "./statementsActions";
import {
  invalidateStatementDiagnosticsRequests,
  refreshStatementDiagnosticsRequests,
} from "src/redux/apiReducers";
import {
  createStatementDiagnosticsAlertLocalSetting,
  cancelStatementDiagnosticsAlertLocalSetting,
} from "src/redux/alerts";
import { TimeScale } from "@cockroachlabs/cluster-ui";
import { setTimeScale } from "src/redux/timeScale";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";

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
  ]);
}
