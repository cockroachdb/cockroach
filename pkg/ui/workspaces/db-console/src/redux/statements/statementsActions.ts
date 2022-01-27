// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Action } from "redux";
import { PayloadAction } from "src/interfaces/action";
import { google } from "@cockroachlabs/crdb-protobuf-client";
import IDuration = google.protobuf.IDuration;
import { TimeScale } from "src/redux/timeScale";

export const CREATE_STATEMENT_DIAGNOSTICS_REPORT =
  "cockroachui/statements/CREATE_STATEMENT_DIAGNOSTICS_REPORT";
export const CREATE_STATEMENT_DIAGNOSTICS_COMPLETE =
  "cockroachui/statements/CREATE_STATEMENT_DIAGNOSTICS_COMPLETE";
export const CREATE_STATEMENT_DIAGNOSTICS_FAILED =
  "cockroachui/statements/CREATE_STATEMENT_DIAGNOSTICS_FAILED";
export const OPEN_STATEMENT_DIAGNOSTICS_MODAL =
  "cockroachui/statements/OPEN_STATEMENT_DIAGNOSTICS_MODAL";
export const CANCEL_STATEMENT_DIAGNOSTICS_REPORT =
  "cockroachui/statements/CANCEL_STATEMENT_DIAGNOSTICS_REPORT";
export const CANCEL_STATEMENT_DIAGNOSTICS_COMPLETE =
  "cockroachui/statements/CANCEL_STATEMENT_DIAGNOSTICS_COMPLETE";
export const CANCEL_STATEMENT_DIAGNOSTICS_FAILED =
  "cockroachui/statements/CANCEL_STATEMENT_DIAGNOSTICS_FAILED";

export type DiagnosticsReportPayload = {
  statementFingerprint: string;
};

export type CreateStatementDiagnosticsReportPayload = {
  statementFingerprint: string;
  minExecLatency: IDuration;
  expiresAfter: IDuration;
};

export function createStatementDiagnosticsReportAction(
  statementFingerprint: string,
  minExecLatency: IDuration,
  expiresAfter: IDuration,
): PayloadAction<CreateStatementDiagnosticsReportPayload> {
  return {
    type: CREATE_STATEMENT_DIAGNOSTICS_REPORT,
    payload: {
      statementFingerprint,
      minExecLatency,
      expiresAfter,
    },
  };
}

export function createStatementDiagnosticsReportCompleteAction(): Action {
  return {
    type: CREATE_STATEMENT_DIAGNOSTICS_COMPLETE,
  };
}

export function createStatementDiagnosticsReportFailedAction(): Action {
  return {
    type: CREATE_STATEMENT_DIAGNOSTICS_FAILED,
  };
}

export type CancelStatementDiagnosticsReportPayload = {
  requestID: Long;
};

export function cancelStatementDiagnosticsReportAction(
  requestID: Long,
): PayloadAction<CancelStatementDiagnosticsReportPayload> {
  return {
    type: CANCEL_STATEMENT_DIAGNOSTICS_REPORT,
    payload: {
      requestID,
    },
  };
}

export function cancelStatementDiagnosticsReportCompleteAction(): Action {
  return {
    type: CANCEL_STATEMENT_DIAGNOSTICS_COMPLETE,
  };
}

export function cancelStatementDiagnosticsReportFailedAction(): Action {
  return {
    type: CANCEL_STATEMENT_DIAGNOSTICS_FAILED,
  };
}

export function createOpenDiagnosticsModalAction(
  statementFingerprint: string,
): PayloadAction<DiagnosticsReportPayload> {
  return {
    type: OPEN_STATEMENT_DIAGNOSTICS_MODAL,
    payload: {
      statementFingerprint,
    },
  };
}

/***************************************
        Combined Stats Actions
****************************************/

export const SET_COMBINED_STATEMENTS_TIME_SCALE =
  "cockroachui/statements/SET_COMBINED_STATEMENTS_TIME_SCALE";

export function setCombinedStatementsTimeScaleAction(
  ts: TimeScale,
): PayloadAction<TimeScale> {
  return {
    type: SET_COMBINED_STATEMENTS_TIME_SCALE,
    payload: ts,
  };
}
