// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import Long from "long";
import {
  AggregateStatistics,
  populateRegionNodeForStatements,
} from "./statementsTable";

describe("populateRegionNodeForStatements", () => {
  function statementWithNodeIDs(...nodeIDs: number[]): AggregateStatistics {
    return {
      aggregatedFingerprintID: "",
      aggregatedFingerprintHexID: "",
      label: "",
      summary: "",
      aggregatedTs: 0,
      aggregationInterval: 0,
      implicitTxn: false,
      fullScan: false,
      database: "",
      applicationName: "",
      stats: { nodes: nodeIDs.map(id => Long.fromInt(id)) },
    };
  }

  it("maps nodes to regions, sorted", () => {
    const statement = statementWithNodeIDs(1, 2);
    populateRegionNodeForStatements([statement], {
      "1": "gcp-us-west1",
      "2": "gcp-us-east1",
    });
    assert.deepEqual(["gcp-us-east1", "gcp-us-west1"], statement.regions);
  });

  it("handles statements without nodes", () => {
    const statement = statementWithNodeIDs();
    populateRegionNodeForStatements([statement], {
      "1": "gcp-us-west1",
      "2": "gcp-us-east1",
    });
    assert.deepEqual(statement.regions, []);
  });

  it("excludes nodes whose region we don't know", () => {
    const statement = statementWithNodeIDs(1, 2);
    populateRegionNodeForStatements([statement], {
      "1": "gcp-us-west1",
    });
    assert.deepEqual(statement.regions, ["gcp-us-west1"]);
  });
});
