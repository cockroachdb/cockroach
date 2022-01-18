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

import { createStatementDiagnosticsReport } from "src/util/api";
import {
  CREATE_STATEMENT_DIAGNOSTICS_REPORT,
  CreateStatementDiagnosticsReportPayload,
  createStatementDiagnosticsReportCompleteAction,
  createStatementDiagnosticsReportFailedAction,
  SET_COMBINED_STATEMENTS_TIME_SCALE,
} from "./statementsActions";
import { cockroach } from "src/js/protos";
import CreateStatementDiagnosticsReportRequest = cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;
import CombinedStatementsRequest = cockroach.server.serverpb.StatementsRequest;
import {
  invalidateStatementDiagnosticsRequests,
  refreshStatementDiagnosticsRequests,
  invalidateStatements,
  refreshStatements,
  invalidateUserSQLRoles,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { createStatementDiagnosticsAlertLocalSetting } from "src/redux/alerts";
import { statementsTimeScaleLocalSetting } from "src/redux/statementsTimeScale";
import { TimeScale, toDateRange } from "@cockroachlabs/cluster-ui";
import Long from "long";

export function* createDiagnosticsReportSaga(
  action: PayloadAction<CreateStatementDiagnosticsReportPayload>,
) {
  const { statementFingerprint, minExecLatency, expiresAfter } = action.payload;
  const createDiagnosticsReportRequest = new CreateStatementDiagnosticsReportRequest(
    {
      statement_fingerprint: statementFingerprint,
      min_execution_latency: minExecLatency,
      expires_after: expiresAfter,
    },
  );
  try {
    yield call(
      createStatementDiagnosticsReport,
      createDiagnosticsReportRequest,
    );
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

export function* setCombinedStatementsTimeScaleSaga(
  action: PayloadAction<TimeScale>,
) {
  const ts = action.payload;

  yield put(statementsTimeScaleLocalSetting.set(ts));
  const [start, end] = toDateRange(ts);
  const req = new CombinedStatementsRequest({
    combined: true,
    start: Long.fromNumber(start.unix()),
    end: Long.fromNumber(end.unix()),
  });
  yield put(invalidateStatements());
  yield put(refreshStatements(req) as any);
  yield put(invalidateUserSQLRoles());
  yield put(refreshUserSQLRoles() as any);
}

export function* statementsSaga() {
  yield all([
    takeEvery(CREATE_STATEMENT_DIAGNOSTICS_REPORT, createDiagnosticsReportSaga),
    takeLatest(
      SET_COMBINED_STATEMENTS_TIME_SCALE,
      setCombinedStatementsTimeScaleSaga,
    ),
  ]);
}
