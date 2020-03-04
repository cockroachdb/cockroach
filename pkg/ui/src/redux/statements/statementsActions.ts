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

export const REQUEST_STATEMENT_DIAGNOSTICS_REPORT = "cockroachui/statements/REQUEST_STATEMENT_DIAGNOSTICS_REPORT";
export const STATEMENT_DIAGNOSTICS_REPORT_REQUEST_COMPLETE = "cockroachui/statements/STATEMENT_DIAGNOSTICS_REPORT_REQUEST_COMPLETE";
export const STATEMENT_DIAGNOSTICS_REPORT_REQUEST_FAILED = "cockroachui/statements/STATEMENT_DIAGNOSTICS_REPORT_REQUEST_FAILED";

export type DiagnosticsPayload = {
  statementFingerprint: string;
};

export function requestStatementDiagnosticsReport(statementFingerprint: string): PayloadAction<DiagnosticsPayload> {
  return {
    type: REQUEST_STATEMENT_DIAGNOSTICS_REPORT,
    payload: {
      statementFingerprint,
    },
  };
}

export function completeStatementDiagnosticsReportRequest(): Action {
  return {
    type: STATEMENT_DIAGNOSTICS_REPORT_REQUEST_COMPLETE,
  };
}

export function failedStatementDiagnosticsReportRequest(): Action {
  return {
    type: STATEMENT_DIAGNOSTICS_REPORT_REQUEST_FAILED,
  };
}
