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

import { createStatementDiagnosticsRequest } from "src/util/api";
import {
  REQUEST_STATEMENT_DIAGNOSTICS,
  DiagnosticsPayload,
  completeStatementDiagnosticsRequest,
  failedStatementDiagnosticsRequest,
} from "./statementsActions";
import { cockroach } from "src/js/protos";
import StatementDiagnosticsRequest = cockroach.server.serverpb.StatementDiagnosticsRequestsRequest;
import { refreshStatementDiagnosticsRequests } from "src/redux/apiReducers";

export function* requestDiagnostics(action: PayloadAction<DiagnosticsPayload>) {
  const { statementFingerprint } = action.payload;
  const statementDiagnosticsRequest = new StatementDiagnosticsRequest({ statement_fingerprint: statementFingerprint });
  try {
    yield call(createStatementDiagnosticsRequest, statementDiagnosticsRequest);
    yield put(completeStatementDiagnosticsRequest());
    yield call(refreshStatementDiagnosticsRequests);
  } catch (e) {
    yield put(failedStatementDiagnosticsRequest());
  }
}

export function* statementsSaga() {
  yield takeEvery(REQUEST_STATEMENT_DIAGNOSTICS, requestDiagnostics);
}
