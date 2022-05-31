// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import { selectDiagnosticsReportsPerStatement } from "./statementDiagnostics.selectors";
import Long from "long";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

const reports: IStatementDiagnosticsReport[] = [
  {
    id: Long.fromNumber(1),
    completed: false,
    statement_fingerprint: "SHOW database",
    statement_diagnostics_id: Long.fromNumber(594413981435920385),
    requested_at: { seconds: Long.fromNumber(100), nanos: 737251000 },
  },
  {
    id: Long.fromNumber(2),
    completed: true,
    statement_fingerprint: "SHOW database",
    statement_diagnostics_id: Long.fromNumber(594413281435920385),
    requested_at: { seconds: Long.fromNumber(200), nanos: 737251000 },
  },
  {
    id: Long.fromNumber(3),
    completed: true,
    statement_fingerprint: "SHOW database",
    statement_diagnostics_id: Long.fromNumber(594413281435920385),
    requested_at: { seconds: Long.fromNumber(300), nanos: 737251000 },
  },
];

describe("statementDiagnostics selectors", () => {
  describe("selectDiagnosticsReportsPerStatement", () => {
    it("returns diagnostics reports sorted in descending order", () => {
      const diagnosticsPerStatement = selectDiagnosticsReportsPerStatement.resultFunc(
        reports,
      );
      assert.deepEqual(
        diagnosticsPerStatement["SHOW database"][0].id,
        Long.fromNumber(3),
      );
    });
  });
});
