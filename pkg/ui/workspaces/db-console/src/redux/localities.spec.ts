// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";

import { selectLocalityTree, LocalityTier } from "./localities";

function makeStateWithLocalities(localities: LocalityTier[][]) {
  const nodes = localities.map((locality, i) => {
    return {
      desc: {
        node_id: i,
        locality: { tiers: locality },
      },
    };
  });

  return {
    cachedData: {
      nodes: {
        data: nodes,
        inFlight: false,
        valid: true,
      },
      liveness: {},
    },
  };
}

describe("selectLocalityTree", function () {
  it("puts nodes without locality at the top-level", function () {
    const state = makeStateWithLocalities([[]]);

    const tree = selectLocalityTree(state);

    assert.isEmpty(tree.tiers);
    assert.isEmpty(tree.localities);

    assert.lengthOf(tree.nodes, 1);
  });

  it("organizes nodes by locality", function () {
    const state = makeStateWithLocalities([
      [{ key: "region", value: "us-east-1" }],
      [{ key: "region", value: "us-east-2" }],
    ]);

    const tree = selectLocalityTree(state);

    assert.isEmpty(tree.tiers);
    assert.isEmpty(tree.nodes);

    assert.hasAllKeys(tree.localities, ["region"]);
    const regions = tree.localities.region;

    assert.hasAllKeys(regions, ["us-east-1", "us-east-2"]);

    const usEast1 = regions["us-east-1"];

    assert.isEmpty(usEast1.localities);
    assert.deepEqual(usEast1.tiers, [{ key: "region", value: "us-east-1" }]);

    assert.lengthOf(usEast1.nodes, 1);

    const usEast2 = regions["us-east-2"];

    assert.isEmpty(usEast2.localities);
    assert.deepEqual(usEast2.tiers, [{ key: "region", value: "us-east-2" }]);

    assert.lengthOf(usEast2.nodes, 1);
  });
});
