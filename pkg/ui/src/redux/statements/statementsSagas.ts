// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { call, put, takeEvery } from "redux-saga/effects";
import { PayloadAction } from "src/interfaces/action";

import { createStatementDiagnosticsReport } from "src/util/api";
import {
  REQUEST_STATEMENT_DIAGNOSTICS_REPORT,
  DiagnosticsPayload,
  completeStatementDiagnosticsReportRequest,
  failedStatementDiagnosticsReportRequest,
} from "./statementsActions";
import { cockroach } from "src/js/protos";
import CreateStatementDiagnosticsReportRequest = cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;
import { refreshStatementDiagnosticsRequests } from "src/redux/apiReducers";

export function* requestDiagnosticsReport(action: PayloadAction<DiagnosticsPayload>) {
  const { statementFingerprint } = action.payload;
  const diagnosticsReportRequest = new CreateStatementDiagnosticsReportRequest({
    statement_fingerprint: statementFingerprint,
  });
  try {
    yield call(createStatementDiagnosticsReport, diagnosticsReportRequest);
    yield put(completeStatementDiagnosticsReportRequest());
    yield call(refreshStatementDiagnosticsRequests);
  } catch (e) {
    yield put(failedStatementDiagnosticsReportRequest());
  }
}

export function* statementsSaga() {
  yield takeEvery(REQUEST_STATEMENT_DIAGNOSTICS_REPORT, requestDiagnosticsReport);
}
