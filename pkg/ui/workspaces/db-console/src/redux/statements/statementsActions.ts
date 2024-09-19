// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { TimeScale, api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { Action } from "redux";

import { PayloadAction } from "src/interfaces/action";

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

export function createStatementDiagnosticsReportAction(
  insertStmtDiagnosticsRequest: clusterUiApi.InsertStmtDiagnosticRequest,
): PayloadAction<clusterUiApi.InsertStmtDiagnosticRequest> {
  return {
    type: CREATE_STATEMENT_DIAGNOSTICS_REPORT,
    payload: insertStmtDiagnosticsRequest,
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

export function cancelStatementDiagnosticsReportAction(
  cancelStmtDiagnosticsRequest: clusterUiApi.CancelStmtDiagnosticRequest,
): PayloadAction<clusterUiApi.CancelStmtDiagnosticRequest> {
  return {
    type: CANCEL_STATEMENT_DIAGNOSTICS_REPORT,
    payload: cancelStmtDiagnosticsRequest,
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

// Setting the timescale using this action type has some additional
// side effects, see statementSagas.ts for the saga function:
export const SET_GLOBAL_TIME_SCALE =
  "cockroachui/statements/SET_GLOBAL_TIME_SCALE";

export function setGlobalTimeScaleAction(
  ts: TimeScale,
): PayloadAction<TimeScale> {
  return {
    type: SET_GLOBAL_TIME_SCALE,
    payload: ts,
  };
}
