// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeEvery } from "redux-saga/effects";

import { terminateQuery, terminateSession } from "src/api/terminateQueryApi";
import { actions as sessionsActions } from "src/store/sessions";

import { maybeError } from "../../util";

import { actions as terminateQueryActions } from "./terminateQuery.reducer";

const CancelSessionRequest = cockroach.server.serverpb.CancelSessionRequest;
const CancelQueryRequest = cockroach.server.serverpb.CancelQueryRequest;
export type ICancelSessionRequest =
  cockroach.server.serverpb.ICancelSessionRequest;
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
    yield put(terminateQueryActions.terminateSessionFailed(maybeError(e)));
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
    yield put(terminateQueryActions.terminateQueryFailed(maybeError(e)));
  }
}

export function* terminateSaga() {
  yield all([
    takeEvery(terminateQueryActions.terminateSession, terminateSessionSaga),
    takeEvery(terminateQueryActions.terminateQuery, terminateQuerySaga),
  ]);
}
