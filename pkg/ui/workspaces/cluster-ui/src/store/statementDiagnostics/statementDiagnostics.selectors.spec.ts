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
import { StatementDiagnosticsReport } from "../../api";
import moment from "moment-timezone";

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
