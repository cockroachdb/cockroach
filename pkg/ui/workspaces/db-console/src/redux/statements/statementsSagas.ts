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
  cancelStatementDiagnosticsReport,
  CancelStatementDiagnosticsReportResponseMessage,
  createStatementDiagnosticsReport,
} from "src/util/api";
import {
  CREATE_STATEMENT_DIAGNOSTICS_REPORT,
  CreateStatementDiagnosticsReportPayload,
  createStatementDiagnosticsReportCompleteAction,
  createStatementDiagnosticsReportFailedAction,
  SET_GLOBAL_TIME_SCALE,
  CANCEL_STATEMENT_DIAGNOSTICS_REPORT,
  cancelStatementDiagnosticsReportCompleteAction,
  cancelStatementDiagnosticsReportFailedAction,
  CancelStatementDiagnosticsReportPayload,
} from "./statementsActions";
import { cockroach } from "src/js/protos";
import CreateStatementDiagnosticsReportRequest = cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;
import CancelStatementDiagnosticsReportRequest = cockroach.server.serverpb.CancelStatementDiagnosticsReportRequest;
import CombinedStatementsRequest = cockroach.server.serverpb.StatementsRequest;
import {
  invalidateStatementDiagnosticsRequests,
  refreshStatementDiagnosticsRequests,
  invalidateStatements,
  refreshStatements,
} from "src/redux/apiReducers";
import {
  createStatementDiagnosticsAlertLocalSetting,
  cancelStatementDiagnosticsAlertLocalSetting,
} from "src/redux/alerts";
import { TimeScale, toRoundedDateRange } from "@cockroachlabs/cluster-ui";
import Long from "long";
import { setTimeScale } from "src/redux/timeScale";

export function* createDiagnosticsReportSaga(
  action: PayloadAction<CreateStatementDiagnosticsReportPayload>,
) {
  const { statementFingerprint, minExecLatency, expiresAfter } = action.payload;
  const createDiagnosticsReportRequest =
    new CreateStatementDiagnosticsReportRequest({
      statement_fingerprint: statementFingerprint,
      min_execution_latency: minExecLatency,
      expires_after: expiresAfter,
    });
  try {
    yield call(
      createStatementDiagnosticsReport,
      createDiagnosticsReportRequest,
    );
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
  action: PayloadAction<CancelStatementDiagnosticsReportPayload>,
) {
  const { requestID } = action.payload;
  const cancelDiagnosticsReportRequest =
    new CancelStatementDiagnosticsReportRequest({
      request_id: requestID,
    });
  try {
    const response: CancelStatementDiagnosticsReportResponseMessage =
      yield call(
        cancelStatementDiagnosticsReport,
        cancelDiagnosticsReportRequest,
      );

    if (response.error !== "") {
      throw response.error;
    }

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
  const [start, end] = toRoundedDateRange(ts);
  const req = new CombinedStatementsRequest({
    combined: true,
    start: Long.fromNumber(start.unix()),
    end: Long.fromNumber(end.unix()),
  });
  yield put(invalidateStatements());
  yield put(refreshStatements(req) as any);
}

export function* statementsSaga() {
  yield all([
    takeEvery(CREATE_STATEMENT_DIAGNOSTICS_REPORT, createDiagnosticsReportSaga),
    takeEvery(CANCEL_STATEMENT_DIAGNOSTICS_REPORT, cancelDiagnosticsReportSaga),
    takeLatest(SET_GLOBAL_TIME_SCALE, setCombinedStatementsTimeScaleSaga),
  ]);
}
