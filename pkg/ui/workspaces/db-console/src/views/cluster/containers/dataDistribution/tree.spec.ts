// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  flatten,
  layoutTreeHorizontal,
  sumValuesUnderPaths,
  TreePath,
  LayoutCell,
} from "./tree";

describe("tree", () => {
  describe("layoutTreeHorizontal", () => {
    it("lays out a simple tree", () => {
      const tree = {
        name: "a",
        data: "a",
        children: [
          { name: "b", data: "b" },
          { name: "c", data: "c" },
        ],
      };

      // |   a   |
      // | b | c |
      const expectedLayout: LayoutCell<string>[][] = [
        [
          {
            width: 2,
            data: "a",
            path: [],
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: false,
          },
        ],
        [
          {
            width: 1,
            path: ["b"],
            data: "b",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
          {
            width: 1,
            path: ["c"],
            data: "c",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
        ],
      ];
      expect(layoutTreeHorizontal(tree, [])).toEqual(expectedLayout);
    });

    it("lays out a tree of inconsistent depth, inserting a placeholder", () => {
      const tree = {
        name: "a",
        data: "a",
        children: [
          { name: "b", data: "b" },
          {
            name: "c",
            data: "c",
            children: [
              { name: "d", data: "d" },
              { name: "e", data: "e" },
            ],
          },
        ],
      };

      // |      a      |
      // | <P> |   c   |
      // |  b  | d | e |
      const expectedLayout = [
        [
          {
            width: 3,
            data: "a",
            path: [],
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: false,
          },
        ],
        [
          {
            width: 1,
            path: ["b"],
            data: "b",
            isCollapsed: false,
            isPlaceholder: true,
            isLeaf: false,
          },
          {
            width: 2,
            data: "c",
            path: ["c"],
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: false,
          },
        ],
        [
          {
            width: 1,
            path: ["b"],
            data: "b",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
          {
            width: 1,
            path: ["c", "d"],
            data: "d",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
          {
            width: 1,
            path: ["c", "e"],
            data: "e",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
        ],
      ];
      const actualLayout = layoutTreeHorizontal(tree, []);
      expect(actualLayout).toEqual(expectedLayout);
    });

    it("inserts placeholders under a collapsed node, if other subtrees are deeper", () => {
      const tree = {
        name: "a",
        data: "a",
        children: [
          {
            name: "b",
            data: "b",
            children: [
              { name: "c", data: "c" },
              { name: "d", data: "d" },
            ],
          },
          {
            name: "e",
            data: "e",
            children: [
              { name: "f", data: "f" },
              { name: "g", data: "g" },
            ],
          },
        ],
      };

      // Without anything collapsed:
      // |       a       |
      // |   b   |   e   |
      // | c | d | f | g |
      const expectedLayout = [
        [
          {
            width: 4,
            data: "a",
            path: [],
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: false,
          },
        ],
        [
          {
            width: 2,
            path: ["b"],
            data: "b",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: false,
          },
          {
            width: 2,
            path: ["e"],
            data: "e",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: false,
          },
        ],
        [
          {
            width: 1,
            path: ["b", "c"],
            data: "c",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
          {
            width: 1,
            path: ["b", "d"],
            data: "d",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
          {
            width: 1,
            path: ["e", "f"],
            data: "f",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
          {
            width: 1,
            path: ["e", "g"],
            data: "g",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
        ],
      ];
      const actualLayout = layoutTreeHorizontal(tree, []);
      expect(actualLayout).toEqual(expectedLayout);

      // Collapse e:
      // |      a      |
      // |   b   |  e  |
      // | c | d | <P> |
      const expectedLayoutCollapseE = [
        [
          {
            width: 3,
            data: "a",
            path: [],
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: false,
          },
        ],
        [
          {
            width: 2,
            path: ["b"],
            data: "b",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: false,
          },
          {
            width: 1,
            path: ["e"],
            data: "e",
            isCollapsed: true,
            isPlaceholder: false,
            isLeaf: false,
          },
        ],
        [
          {
            width: 1,
            path: ["b", "c"],
            data: "c",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
          {
            width: 1,
            path: ["b", "d"],
            data: "d",
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: true,
          },
          {
            width: 1,
            path: ["e"],
            data: "e",
            isCollapsed: false,
            isPlaceholder: true,
            isLeaf: false,
          },
        ],
      ];
      const actualLayoutCollapseE = layoutTreeHorizontal(tree, [["e"]]);
      expect(actualLayoutCollapseE).toEqual(expectedLayoutCollapseE);

      // Collapse e and b:
      // |     a     |
      // |  b  |  e  |
      const expectedLayoutCollapseBE: LayoutCell<string>[][] = [
        [
          {
            width: 2,
            data: "a",
            path: [],
            isCollapsed: false,
            isPlaceholder: false,
            isLeaf: false,
          },
        ],
        [
          {
            width: 1,
            path: ["b"],
            data: "b",
            isCollapsed: true,
            isPlaceholder: false,
            isLeaf: false,
          },
          {
            width: 1,
            path: ["e"],
            data: "e",
            isCollapsed: true,
            isPlaceholder: false,
            isLeaf: false,
          },
        ],
      ];
      const actualLayoutCollapseBE = layoutTreeHorizontal(tree, [["b"], ["e"]]);
      expect(actualLayoutCollapseBE).toEqual(expectedLayoutCollapseBE);
    });
  });

  describe("flatten", () => {
    const tree = {
      name: "a",
      data: "a",
      children: [
        {
          name: "b",
          data: "b",
          children: [
            { name: "c", data: "c" },
            { name: "d", data: "d" },
          ],
        },
        {
          name: "e",
          data: "e",
          children: [
            { name: "f", data: "f" },
            { name: "g", data: "g" },
          ],
        },
      ],
    };

    describe("with includeNodes = true", () => {
      it("lays out a tree with nothing collapsed", () => {
        const actualFlattened = flatten(tree, [], true);
        const expectedFlattened = [
          { depth: 0, isLeaf: false, isCollapsed: false, data: "a", path: [] },
          {
            depth: 1,
            isLeaf: false,
            isCollapsed: false,
            data: "b",
            path: ["b"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "c",
            path: ["b", "c"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "d",
            path: ["b", "d"],
          },
          {
            depth: 1,
            isLeaf: false,
            isCollapsed: false,
            data: "e",
            path: ["e"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "f",
            path: ["e", "f"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "g",
            path: ["e", "g"],
          },
        ];

        expect(actualFlattened).toEqual(expectedFlattened);
      });

      it("lays out a tree with a node collapsed", () => {
        const actualFlattened = flatten(tree, [["b"]], true);
        const expectedFlattened = [
          { depth: 0, isLeaf: false, isCollapsed: false, data: "a", path: [] },
          {
            depth: 1,
            isLeaf: false,
            isCollapsed: true,
            data: "b",
            path: ["b"],
          },
          {
            depth: 1,
            isLeaf: false,
            isCollapsed: false,
            data: "e",
            path: ["e"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "f",
            path: ["e", "f"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "g",
            path: ["e", "g"],
          },
        ];

        expect(actualFlattened).toEqual(expectedFlattened);
      });
    });

    describe("with includeNodes = false", () => {
      it("lays out a tree with nothing collapsed", () => {
        const actualFlattened = flatten(tree, [], false);
        const expectedFlattened = [
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "c",
            path: ["b", "c"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "d",
            path: ["b", "d"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "f",
            path: ["e", "f"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "g",
            path: ["e", "g"],
          },
        ];

        expect(actualFlattened).toEqual(expectedFlattened);
      });

      it("lays out a tree with a node collapsed", () => {
        const actualFlattened = flatten(tree, [["b"]], false);
        const expectedFlattened = [
          {
            depth: 1,
            isLeaf: false,
            isCollapsed: true,
            data: "b",
            path: ["b"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "f",
            path: ["e", "f"],
          },
          {
            depth: 2,
            isLeaf: true,
            isCollapsed: false,
            data: "g",
            path: ["e", "g"],
          },
        ];

        expect(actualFlattened).toEqual(expectedFlattened);
      });
    });
  });

  describe("sumValuesUnderPaths", () => {
    // |       |    C_1    |
    // |       | C_2 | C_3 |
    // |-------|-----|-----|
    // | R_a   |     |     |
    // |   R_b |  1  |  2  |
    // |   R_c |  3  |  4  |

    const rowTree = {
      name: "a",
      children: [{ name: "b" }, { name: "c" }],
    };
    const colTree = {
      name: "1",
      children: [{ name: "2" }, { name: "3" }],
    };
    // by row, then col.
    const values: { [name: string]: { [name: string]: number } } = {
      b: { "2": 1, "3": 2 },
      c: { "2": 3, "3": 4 },
    };
    function getValue(rowPath: TreePath, colPath: TreePath): number {
      return values[rowPath[0]][colPath[0]];
    }

    it("computes a sum for the roots of both trees", () => {
      const actualSum = sumValuesUnderPaths(rowTree, colTree, [], [], getValue);
      const expectedSum = 1 + 2 + 3 + 4;
      expect(actualSum).toEqual(expectedSum);
    });

    it("computes a sum for the root of one tree and the leaf of another", () => {
      const actualSum = sumValuesUnderPaths(
        rowTree,
        colTree,
        ["b"],
        [],
        getValue,
      );
      const expectedSum = 1 + 2;
      expect(actualSum).toEqual(expectedSum);
    });

    it("computes a sum for a single cell (two leaves)", () => {
      const actualSum = sumValuesUnderPaths(
        rowTree,
        colTree,
        ["b"],
        ["3"],
        getValue,
      );
      const expectedSum = 2;
      expect(actualSum).toEqual(expectedSum);
    });
  });
});
