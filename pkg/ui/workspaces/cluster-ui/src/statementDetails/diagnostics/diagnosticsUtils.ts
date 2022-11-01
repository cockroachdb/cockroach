// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { isUndefined } from "lodash";
import { DiagnosticStatuses } from "src/statementsDiagnostics";
import { StatementDiagnosticsReport } from "../../api";
import moment from "moment";

export function getDiagnosticsStatus(
  diagnosticsRequest: StatementDiagnosticsReport,
): DiagnosticStatuses {
  if (diagnosticsRequest.completed) {
    return "READY";
  }

  return "WAITING";
}

export function sortByRequestedAtField(
  a: StatementDiagnosticsReport,
  b: StatementDiagnosticsReport,
): number {
  const activatedOnA = moment(a.requested_at)?.unix();
  const activatedOnB = moment(b.requested_at)?.unix();
  if (isUndefined(activatedOnA) && isUndefined(activatedOnB)) {
    return 0;
  }
  if (activatedOnA < activatedOnB) {
    return -1;
  }
  if (activatedOnA > activatedOnB) {
    return 1;
  }
  return 0;
}

export function sortByCompletedField(
  a: StatementDiagnosticsReport,
  b: StatementDiagnosticsReport,
): number {
  const completedA = a.completed ? 1 : -1;
  const completedB = b.completed ? 1 : -1;
  if (completedA < completedB) {
    return -1;
  }
  if (completedA > completedB) {
    return 1;
  }
  return 0;
}

export function sortByStatementFingerprintField(
  a: StatementDiagnosticsReport,
  b: StatementDiagnosticsReport,
): number {
  const statementFingerprintA = a.statement_fingerprint;
  const statementFingerprintB = b.statement_fingerprint;
  if (
    isUndefined(statementFingerprintA) &&
    isUndefined(statementFingerprintB)
  ) {
    return 0;
  }
  if (statementFingerprintA < statementFingerprintB) {
    return -1;
  }
  if (statementFingerprintA > statementFingerprintB) {
    return 1;
  }
  return 0;
}
