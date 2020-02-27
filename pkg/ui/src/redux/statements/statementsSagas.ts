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

import { enqueueStatementDiagnostics } from "src/util/api";
import { ENQUEUE_DIAGNOSTICS, EnqueueDiagnosticsPayload, completeEnqueueDiagnostics } from "./statementsActions";

export function* requestDiagnostics(action: PayloadAction<EnqueueDiagnosticsPayload>) {
  const { statementId } = action.payload;

  yield call(enqueueStatementDiagnostics, {
    statementId,
  });
  yield put(completeEnqueueDiagnostics());
}

export function* statementsSaga() {
  yield takeEvery(ENQUEUE_DIAGNOSTICS, requestDiagnostics);
}
