// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { isUndefined } from "lodash";

import { cockroach } from "src/js/protos";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
import {DiagnosticStatuses} from "src/views/statements/diagnostics/diagnosticStatuses";

export function getDiagnosticsStatus(diagnosticsRequest: IStatementDiagnosticsReport): DiagnosticStatuses {
  if (diagnosticsRequest.completed) {
    return "READY";
  }

  return "WAITING FOR QUERY";
}

export function sortByRequestedAtField(a: IStatementDiagnosticsReport, b: IStatementDiagnosticsReport) {
  const activatedOnA = a.requested_at?.seconds?.toNumber();
  const activatedOnB = b.requested_at?.seconds?.toNumber();
  if (isUndefined(activatedOnA) && isUndefined(activatedOnB)) { return 0; }
  if (activatedOnA < activatedOnB) { return -1; }
  if (activatedOnA > activatedOnB) { return 1; }
  return 0;
}

export function sortByCompletedField(a: IStatementDiagnosticsReport, b: IStatementDiagnosticsReport) {
  const completedA = a.completed ? 1 : -1;
  const completedB = b.completed ? 1 : -1;
  if (completedA < completedB) { return -1; }
  if (completedA > completedB) { return 1; }
  return 0;
}

export function sortByStatementFingerprintField(a: IStatementDiagnosticsReport, b: IStatementDiagnosticsReport) {
  const statementFingerprintA = a.statement_fingerprint;
  const statementFingerprintB = b.statement_fingerprint;
  if (isUndefined(statementFingerprintA) && isUndefined(statementFingerprintB)) { return 0; }
  if (statementFingerprintA < statementFingerprintB) { return -1; }
  if (statementFingerprintA > statementFingerprintB) { return 1; }
  return 0;
}
