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
import { Moment } from "moment";

export const CREATE_STATEMENT_DIAGNOSTICS_REPORT =
  "cockroachui/statements/CREATE_STATEMENT_DIAGNOSTICS_REPORT";
export const CREATE_STATEMENT_DIAGNOSTICS_COMPLETE =
  "cockroachui/statements/CREATE_STATEMENT_DIAGNOSTICS_COMPLETE";
export const CREATE_STATEMENT_DIAGNOSTICS_FAILED =
  "cockroachui/statements/CREATE_STATEMENT_DIAGNOSTICS_FAILED";
export const OPEN_STATEMENT_DIAGNOSTICS_MODAL =
  "cockroachui/statements/OPEN_STATEMENT_DIAGNOSTICS_MODAL";

export type DiagnosticsReportPayload = {
  statementFingerprint: string;
};

export function createStatementDiagnosticsReportAction(
  statementFingerprint: string,
): PayloadAction<DiagnosticsReportPayload> {
  return {
    type: CREATE_STATEMENT_DIAGNOSTICS_REPORT,
    payload: {
      statementFingerprint,
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

export const SET_COMBINED_STATEMENTS_RANGE =
  "cockroachui/statements/SET_COMBINED_STATEMENTS_RANGE";

export type CombinedStatementsPayload = {
  start: Moment;
  end: Moment;
};

export function setCombinedStatementsDateRangeAction(
  start: Moment,
  end: Moment,
): PayloadAction<CombinedStatementsPayload> {
  return {
    type: SET_COMBINED_STATEMENTS_RANGE,
    payload: {
      start,
      end,
    },
  };
}
