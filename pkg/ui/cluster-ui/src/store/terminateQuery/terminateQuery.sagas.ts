// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeEvery } from "redux-saga/effects";

import { terminateQuery, terminateSession } from "src/api/terminateQueryApi";
import { actions as sessionsActions } from "src/store/sessions";
import { actions as terminateQueryActions } from "./terminateQuery.reducer";

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

const CancelSessionRequest = cockroach.server.serverpb.CancelSessionRequest;
const CancelQueryRequest = cockroach.server.serverpb.CancelQueryRequest;
export type ICancelSessionRequest = cockroach.server.serverpb.ICancelSessionRequest;
export type ICancelQueryRequest = cockroach.server.serverpb.ICancelQueryRequest;

export function* terminateSessionSaga(
  action: PayloadAction<ICancelSessionRequest>,
) {
  const terminateSessionRequest = new CancelSessionRequest(action.payload);
  try {
    yield call(terminateSession, terminateSessionRequest);
    yield put(terminateQueryActions.terminateSessionCompleted());
    yield put(sessionsActions.invalidated());
    yield put(sessionsActions.refresh());
  } catch (e) {
    yield put(terminateQueryActions.terminateSessionFailed(e));
  }
}

export function* terminateQuerySaga(
  action: PayloadAction<ICancelQueryRequest>,
) {
  const terminateQueryRequest = new CancelQueryRequest(action.payload);
  try {
    yield call(terminateQuery, terminateQueryRequest);
    yield put(terminateQueryActions.terminateQueryCompleted());
    yield put(sessionsActions.invalidated());
    yield put(sessionsActions.refresh());
  } catch (e) {
    yield put(terminateQueryActions.terminateQueryFailed(e));
  }
}

export function* terminateSaga() {
  yield all([
    takeEvery(terminateQueryActions.terminateSession, terminateSessionSaga),
    takeEvery(terminateQueryActions.terminateQuery, terminateQuerySaga),
  ]);
}
