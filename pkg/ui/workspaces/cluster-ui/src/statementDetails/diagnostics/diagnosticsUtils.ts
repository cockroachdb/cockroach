// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { TimeScale, toDateRange } from "src/timeScaleDropdown";
import { DiagnosticStatuses } from "src/statementsDiagnostics";
import { StatementDiagnosticsReport } from "../../api";
import moment from "moment-timezone";

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
