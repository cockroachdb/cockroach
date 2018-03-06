import { assert } from "chai";

import { showInstructionsBox } from "src/views/clusterviz/containers/map/nodeCanvas";
import { LocalityTree } from "src/redux/localities";

describe("showInstructionsBox", () => {

  const localityTree: LocalityTree = {
    nodes: [],
    tiers: [],
    localities: {
      "region": {
        "us-east-1": {
          nodes: [
            {
              desc: { node_id: 1 },
            },
            {
              desc: { node_id: 2 },
            },
          ],
          localities: {},
          tiers: [{ key: "region", value: "us-east-1" }],
        },
        "us-west-1": {
          nodes: [
            {
              desc: { node_id: 1 },
            },
            {
              desc: { node_id: 2 },
            },
          ],
          localities: {},
          tiers: [{ key: "region", value: "us-west-1" }],
        },
      },
    },
  };

  it("returns false when showMap is true", () => {
    assert.isFalse(showInstructionsBox(true, [], localityTree));
  });

  it("returns true when there are localities", () => {
    assert.isFalse(showInstructionsBox(false, [], localityTree));
  });

  it("returns false when we're down a level and looking at all nodes", () => {
    assert.isFalse(showInstructionsBox(
      false,
      [{ key: "region", value: "us-east-1" }],
      localityTree.localities["region"]["us-east-1"],
    ));
  });

  it("returns false when there are just a bunch of nodes at the top level", () => {
    assert.isFalse(showInstructionsBox(false, [], localityTree));
  });

});
