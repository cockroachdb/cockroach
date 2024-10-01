// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";
import moment from "moment-timezone";

import { StatementDiagnosticsReport } from "../../api";

import { selectDiagnosticsReportsPerStatement } from "./statementDiagnostics.selectors";

const reports: StatementDiagnosticsReport[] = [
  {
    id: "1",
    completed: false,
    statement_fingerprint: "SHOW database",
    statement_diagnostics_id: "594413981435920385",
    requested_at: moment(100, "s"),
  },
  {
    id: "2",
    completed: true,
    statement_fingerprint: "SHOW database",
    statement_diagnostics_id: "594413281435920385",
    requested_at: moment(200, "s"),
  },
  {
    id: "3",
    completed: true,
    statement_fingerprint: "SHOW database",
    statement_diagnostics_id: "594413281435920385",
    requested_at: moment(300, "s"),
  },
];

describe("statementDiagnostics selectors", () => {
  describe("selectDiagnosticsReportsPerStatement", () => {
    it("returns diagnostics reports sorted in descending order", () => {
      const diagnosticsPerStatement =
        selectDiagnosticsReportsPerStatement.resultFunc(reports);
      assert.deepEqual(diagnosticsPerStatement["SHOW database"][0].id, "3");
    });
  });
});
