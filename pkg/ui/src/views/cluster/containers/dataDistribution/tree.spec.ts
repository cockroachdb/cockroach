import { assert } from "chai";
import diff from "deep-diff";

import {
  layoutTree,
} from "./tree";

describe("tree", () => {

  describe("layoutTree", () => {

    it("lays out a simple tree", () => {
      const tree = {
        name: "a", data: "a",
        children: [
          { name: "b", data: "b", },
          { name: "c", data: "c", },
        ],
      };
      const expectedLayout = [
        [ { width: 2, depth: 2, name: "a", path: [], isCollapsed: false, isPlaceholder: false } ],
        [
          { width: 1, depth: 1, path: ["b"], data: "b", isCollapsed: false, isPlaceholder: false },
          { width: 1, depth: 1, path: ["c"], data: "c", isCollapsed: false, isPlaceholder: false },
        ],
      ];
      assert.deepEqual(layoutTree(tree, []), expectedLayout);
    });

    it("lays out a non-rectangular tree, inserting a placeholder", () => {
      const tree = {
        name: "a", data: "a",
        children: [
          { name: "b", data: "b", },
          {
            name: "c", data: "c",
            children: [
              { name: "d", data: "d" },
              { name: "e", data: "e" },
            ],
          },
        ],
      };
      const expectedLayout = [
        [ { width: 3, depth: 3, name: "a", path: [], isCollapsed: false, isPlaceholder: false } ],
        [
          { width: 1, depth: 1, path: ["b"], isCollapsed: false, isPlaceholder: false, data: "b" },
          { width: 2, depth: 2, name: "c", path: ["c"], isCollapsed: false, isPlaceholder: false },
        ],
        [
          // TODO(vilterp): this isn't right... there should be a placeholder here
          { width: 1, depth: 1, path: ["c", "d"], isCollapsed: false, isPlaceholder: false, data: "d" },
          { width: 1, depth: 1, path: ["c", "e"], isCollapsed: false, isPlaceholder: false, data: "e" },
        ],
      ];
      const actualLayout = layoutTree(tree, []);
      assert.deepEqual(actualLayout, expectedLayout);
    });

  });

  describe("flatten", () => {

  });

  describe("setAtPath", () => {

  });

  describe("nodeAtPath", () => {

  });

  describe("getLeafPathsUnderPath", () => {

  });

  describe("getLeafPaths", () => {

  });

  describe("sumValuesUnderPaths", () => {

  });

});
