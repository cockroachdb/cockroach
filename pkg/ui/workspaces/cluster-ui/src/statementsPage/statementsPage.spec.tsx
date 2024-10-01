// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";

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

    assert.equal(filterBySearchQuery(statement, "select"), true);
    assert.equal(filterBySearchQuery(statement, "count"), true);
    assert.equal(filterBySearchQuery(statement, "select count"), true);
    assert.equal(filterBySearchQuery(statement, "cluster settings"), true);

    // Searching by plan should be false.
    assert.equal(filterBySearchQuery(statement, "virtual table"), false);
    assert.equal(filterBySearchQuery(statement, "group (scalar)"), false);
    assert.equal(filterBySearchQuery(statement, "node_build_info"), false);
    assert.equal(filterBySearchQuery(statement, "crdb_internal"), false);
  });
});
