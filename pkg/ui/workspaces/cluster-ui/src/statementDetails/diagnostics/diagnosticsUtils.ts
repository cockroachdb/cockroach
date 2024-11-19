// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { DiagnosticStatuses } from "src/statementsDiagnostics";
import { TimeScale, toDateRange } from "src/timeScaleDropdown";

import { StatementDiagnosticsReport } from "../../api";

export function getDiagnosticsStatus(
  diagnosticsRequest: StatementDiagnosticsReport,
): DiagnosticStatuses {
  if (diagnosticsRequest.completed) {
    return "READY";
  }
  return "WAITING";
}

export function filterByTimeScale(
  diagnostics: StatementDiagnosticsReport[],
  ts: TimeScale,
): StatementDiagnosticsReport[] {
  const [start, end] = toDateRange(ts);
  return diagnostics.filter(
    diag =>
      start.isSameOrBefore(moment(diag.requested_at)) &&
      end.isSameOrAfter(moment(diag.requested_at)),
  );
}
