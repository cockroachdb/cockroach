import { assert } from "chai";

import { getNextTodo, showInstructionsBox } from "src/views/clusterviz/components/instructionsBox";
import { LocalityTier, LocalityTree } from "src/redux/localities";
import { cockroach } from "src/js/protos";
type NodeStatus$Properties = cockroach.server.status.NodeStatus$Properties;

const localityTree: LocalityTree = {
  // No nodes at top level.
  nodes: [],
  tiers: [],
  localities: {
    "region": {
      "us-east-1": {
        // Mixture: some child nodes, some child localities.
        tiers: [{ key: "region", value: "us-east-1" }],
        nodes: [
          { desc: { node_id: 1 } },
          { desc: { node_id: 2 } },
        ],
        localities: {
          "az": {
            "us-east-1a": {
              tiers: [
                { key: "region", value: "us-east-1" },
                { key: "az", value: "us-east-1a" },
              ],
              nodes: [
                { desc: { node_id: 3 } },
                { desc: { node_id: 4 } },
              ],
              localities: {},
            },
          },
        },
      },
      "us-west-1": {
        // Just child nodes.
        tiers: [{ key: "region", value: "us-west-1" }],
        nodes: [
          { desc: { node_id: 5 } },
          { desc: { node_id: 6 } },
        ],
        localities: {},
      },
    },
  },
};

describe("InstructionsBox component", () => {

  describe("showInstructionsBox", () => {

    describe("when we're showing the map", () => {
      it("returns false", () => {
        assert.isFalse(showInstructionsBox(true, [], localityTree));
      });
    });

    describe("when we're not showing the map", () => {

      interface TestCase {
        tiers: LocalityTier[];
        tree: LocalityTree;
        expected: boolean;
        desc: string;
      }

      const cases: TestCase[] = [
        {
          desc: "At top level, all localities.",
          tiers: [],
          tree: localityTree,
          expected: true,
        },
        {
          desc: "At top level, all nodes.",
          tiers: [],
          tree: localityTree.localities.region["us-west-1"],
          expected: true,
        },
        {
          desc: "Down a level, all localities.",
          tiers: [{key: "region", value: "us-made-up-1"}],
          tree: localityTree,
          expected: true,
        },
        {
          desc: "Down a level, all nodes.",
          tiers: [{key: "region", value: "us-west-1"}],
          tree: localityTree.localities.region["us-west-1"],
          expected: false,
        },
        {
          desc: "Down a level, mix of localities and nodes.",
          tiers: [{key: "region", value: "us-east-1"}],
          tree: localityTree.localities.region["us-east-1"],
          expected: true,
        },
      ];

      cases.forEach((testCase) => {
        it(`returns ${testCase.expected} for case "${testCase.desc}"`, () => {
          assert.equal(showInstructionsBox(false, testCase.tiers, testCase.tree), testCase.expected);
        });
      });

    });

  });

  describe("getNextTodo", () => {

    it("returns step one when there are nodes without a locality", () => {
      const allNodes: NodeStatus$Properties[] = [
        { desc: { node_id: 1, locality: { tiers: [] } } },
        { desc: { node_id: 2, locality: { tiers: [{ key: "region", value: "us-east-1" }] } } },
      ];

      const actual = getNextTodo(allNodes);
      // TODO(vilterp: test against actual text.
      assert.equal(actual.num, 1);
    });

    it("returns step two when all nodes have a locality", () => {
      const allNodes: NodeStatus$Properties[] = [
        { desc: { node_id: 1, locality: { tiers: [{ key: "region", value: "us-west-1" }] } } },
        { desc: { node_id: 2, locality: { tiers: [{ key: "region", value: "us-east-1" }] } } },
      ];

      const actual = getNextTodo(allNodes);
      // TODO(vilterp: test against actual text.
      assert.equal(actual.num, 2);
    });

  });

});
