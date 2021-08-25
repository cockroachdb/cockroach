// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeEvery } from "redux-saga/effects";
import { PayloadAction } from "src/interfaces/action";

import { createStatementDiagnosticsReport } from "src/util/api";
import {
  CREATE_STATEMENT_DIAGNOSTICS_REPORT,
  DiagnosticsReportPayload,
  createStatementDiagnosticsReportCompleteAction,
  createStatementDiagnosticsReportFailedAction,
} from "./statementsActions";
import { cockroach } from "src/js/protos";
import CreateStatementDiagnosticsReportRequest = cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;
import {
  invalidateStatementDiagnosticsRequests,
  refreshStatementDiagnosticsRequests,
} from "src/redux/apiReducers";
import { createStatementDiagnosticsAlertLocalSetting } from "src/redux/alerts";

export function* createDiagnosticsReportSaga(
  action: PayloadAction<DiagnosticsReportPayload>,
) {
  const { statementFingerprint } = action.payload;
  const diagnosticsReportRequest = new CreateStatementDiagnosticsReportRequest({
    statement_fingerprint: statementFingerprint,
  });
  try {
    yield call(createStatementDiagnosticsReport, diagnosticsReportRequest);
    yield put(createStatementDiagnosticsReportCompleteAction());
    yield put(invalidateStatementDiagnosticsRequests());
    // PUT expects action with `type` field which isn't defined in `refresh` ThunkAction interface
    yield put(refreshStatementDiagnosticsRequests() as any);
    yield put(
      createStatementDiagnosticsAlertLocalSetting.set({
        show: true,
        status: "SUCCESS",
      }),
    );
  } catch (e) {
    yield put(createStatementDiagnosticsReportFailedAction());
    yield put(
      createStatementDiagnosticsAlertLocalSetting.set({
        show: true,
        status: "FAILED",
      }),
    );
  }
}

export function* statementsSaga() {
  yield all([
    takeEvery(CREATE_STATEMENT_DIAGNOSTICS_REPORT, createDiagnosticsReportSaga),
  ]);
}
