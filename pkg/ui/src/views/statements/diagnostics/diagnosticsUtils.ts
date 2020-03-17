// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "src/js/protos";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
import {DiagnosticStatuses} from "src/views/statements/diagnostics/diagnosticStatuses";

export function getDiagnosticsStatus(diagnosticsRequest: IStatementDiagnosticsReport): DiagnosticStatuses {
  if (diagnosticsRequest.completed) {
    return "READY";
  }

  return "WAITING FOR QUERY";
}
