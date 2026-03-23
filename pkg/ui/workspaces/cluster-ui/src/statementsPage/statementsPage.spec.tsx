// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { filterBySearchQuery } from "src/sqlActivity/util";

import { FlatPlanNode } from "../statementDetails";
import { AggregateStatistics } from "../statementsTable";

describe("StatementsPage", () => {
  test("filterBySearchQuery", () => {
    const testPlanNode: FlatPlanNode = {
      name: "render",
      attrs: [],
      children: [
        {
          name: "group (scalar)",
          attrs: [],
          children: [
            {
              name: "filter",
              attrs: [
                {
                  key: "filter",
                  values: ["variable = _"],
                  warn: false,
                },
              ],
              children: [
                {
                  name: "virtual table",
                  attrs: [
                    {
                      key: "table",
                      values: ["cluster_settings@primary"],
                      warn: false,
                    },
                  ],
                  children: [],
                },
              ],
            },
          ],
        },
      ],
    };

    const statement: AggregateStatistics = {
      aggregatedFingerprintID: "",
      aggregatedFingerprintHexID: "",
      aggregatedTs: 0,
      database: "",
      applicationName: "",
      fullScan: false,
      implicitTxn: false,
      summary: "",
      label:
        "SELECT count(*) > _ FROM [SHOW ALL CLUSTER SETTINGS] AS _ (v) WHERE v = '_'",
      stats: {
        sensitive_info: {
          most_recent_plan_description: testPlanNode,
        },
      },
    };

    expect(filterBySearchQuery(statement, "select")).toBe(true);
    expect(filterBySearchQuery(statement, "count")).toBe(true);
    expect(filterBySearchQuery(statement, "select count")).toBe(true);
    expect(filterBySearchQuery(statement, "cluster settings")).toBe(true);

    // Searching by plan should be false.
    expect(filterBySearchQuery(statement, "virtual table")).toBe(false);
    expect(filterBySearchQuery(statement, "group (scalar)")).toBe(false);
    expect(filterBySearchQuery(statement, "node_build_info")).toBe(false);
    expect(filterBySearchQuery(statement, "crdb_internal")).toBe(false);
  });
});
