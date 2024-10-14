// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Action } from "redux";
import { all, call, put, takeEvery } from "redux-saga/effects";

import { PayloadAction } from "src/interfaces/action";
import { cockroach } from "src/js/protos";
import {
  terminateQueryAlertLocalSetting,
  terminateSessionAlertLocalSetting,
} from "src/redux/alerts";
import { invalidateSessions, refreshSessions } from "src/redux/apiReducers";
import { terminateQuery, terminateSession } from "src/util/api";

import ICancelSessionRequest = cockroach.server.serverpb.ICancelSessionRequest;
import CancelSessionRequest = cockroach.server.serverpb.CancelSessionRequest;
import ICancelQueryRequest = cockroach.server.serverpb.ICancelQueryRequest;
import CancelQueryRequest = cockroach.server.serverpb.CancelQueryRequest;

const TERMINATE_SESSION = "cockroachui/sessions/TERMINATE_SESSION";
const TERMINATE_SESSION_COMPLETE =
  "cockroachui/sessions/TERMINATE_SESSION_COMPLETE";
const TERMINATE_SESSION_FAILED =
  "cockroachui/sessions/TERMINATE_SESSION_FAILED";

const TERMINATE_QUERY = "cockroachui/sessions/TERMINATE_QUERY";
const TERMINATE_QUERY_COMPLETE =
  "cockroachui/sessions/TERMINATE_QUERY_COMPLETE";
const TERMINATE_QUERY_FAILED = "cockroachui/sessions/TERMINATE_QUERY_FAILED";

export function terminateSessionAction(
  req: ICancelSessionRequest,
): PayloadAction<ICancelSessionRequest> {
  return {
    type: TERMINATE_SESSION,
    payload: req,
  };
}

export function terminateSessionCompleteAction(): Action {
  return { type: TERMINATE_SESSION_COMPLETE };
}

export function terminateSessionFailedAction(): Action {
  return { type: TERMINATE_SESSION_FAILED };
}

export function terminateQueryAction(
  req: ICancelQueryRequest,
): PayloadAction<ICancelQueryRequest> {
  return {
    type: TERMINATE_QUERY,
    payload: req,
  };
}

export function terminateQueryCompleteAction(): Action {
  return { type: TERMINATE_QUERY_COMPLETE };
}

export function terminateQueryFailedAction(): Action {
  return { type: TERMINATE_QUERY_FAILED };
}

export function* terminateSessionSaga(
  action: PayloadAction<ICancelSessionRequest>,
) {
  const terminateSessionRequest = new CancelSessionRequest(action.payload);
  try {
    yield call(terminateSession, terminateSessionRequest);
    yield put(terminateSessionCompleteAction());
    yield put(invalidateSessions());
    // PUT expects action with `type` field which isn't defined in `refresh` ThunkAction interface
    yield put(refreshSessions() as any);
    yield put(
      terminateSessionAlertLocalSetting.set({ show: true, status: "SUCCESS" }),
    );
  } catch (e) {
    yield put(terminateSessionFailedAction());
    yield put(
      terminateSessionAlertLocalSetting.set({ show: true, status: "FAILED" }),
    );
  }
}

export function* terminateQuerySaga(
  action: PayloadAction<ICancelQueryRequest>,
) {
  const terminateQueryRequest = new CancelQueryRequest(action.payload);
  try {
    yield call(terminateQuery, terminateQueryRequest);
    yield put(terminateQueryCompleteAction());
    yield put(invalidateSessions());
    // PUT expects action with `type` field which isn't defined in `refresh` ThunkAction interface
    yield put(refreshSessions() as any);
    yield put(
      terminateQueryAlertLocalSetting.set({ show: true, status: "SUCCESS" }),
    );
  } catch (e) {
    yield put(terminateQueryFailedAction());
    yield put(
      terminateQueryAlertLocalSetting.set({ show: true, status: "FAILED" }),
    );
  }
}

export function* sessionsSaga() {
  yield all([
    takeEvery(TERMINATE_SESSION, terminateSessionSaga),
    takeEvery(TERMINATE_QUERY, terminateQuerySaga),
  ]);
}
