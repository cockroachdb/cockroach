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

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import {
  FlatPlanNode,
  FlatPlanNodeAttribute,
  flattenTree,
  flattenAttributes,
} from "./planView";
import IAttr = cockroach.sql.ExplainTreePlanNode.IAttr;
import IExplainTreePlanNode = cockroach.sql.IExplainTreePlanNode;

const testAttrs1: IAttr[] = [
  {
    key: "key1",
    value: "value1",
  },
  {
    key: "key2",
    value: "value2",
  },
];

const testAttrs2: IAttr[] = [
  {
    key: "key3",
    value: "value3",
  },
  {
    key: "key4",
    value: "value4",
  },
];

const testFlatAttrs1: FlatPlanNodeAttribute[] = [
  {
    key: "key1",
    values: ["value1"],
    warn: false,
  },
  {
    key: "key2",
    values: ["value2"],
    warn: false,
  },
];

const testFlatAttrs2: FlatPlanNodeAttribute[] = [
  {
    key: "key3",
    values: ["value3"],
    warn: false,
  },
  {
    key: "key4",
    values: ["value4"],
    warn: false,
  },
];

const treePlanWithSingleChildPaths: IExplainTreePlanNode = {
  name: "root",
  attrs: null,
  children: [
    {
      name: "single_grandparent",
      attrs: testAttrs1,
      children: [
        {
          name: "single_parent",
          attrs: null,
          children: [
            {
              name: "single_child",
              attrs: testAttrs2,
              children: [],
            },
          ],
        },
      ],
    },
  ],
};

const expectedFlatPlanWithSingleChildPaths: FlatPlanNode[] = [
  {
    name: "root",
    attrs: [],
    children: [],
  },
  {
    name: "single_grandparent",
    attrs: testFlatAttrs1,
    children: [],
  },
  {
    name: "single_parent",
    attrs: [],
    children: [],
  },
  {
    name: "single_child",
    attrs: testFlatAttrs2,
    children: [],
  },
];

const treePlanWithChildren1: IExplainTreePlanNode = {
  name: "root",
  attrs: testAttrs1,
  children: [
    {
      name: "single_grandparent",
      attrs: testAttrs1,
      children: [
        {
          name: "parent_1",
          attrs: null,
          children: [
            {
              name: "single_child",
              attrs: testAttrs2,
              children: [],
            },
          ],
        },
        {
          name: "parent_2",
          attrs: null,
          children: [],
        },
      ],
    },
  ],
};

const expectedFlatPlanWithChildren1: FlatPlanNode[] = [
  {
    name: "root",
    attrs: testFlatAttrs1,
    children: [],
  },
  {
    name: "single_grandparent",
    attrs: testFlatAttrs1,
    children: [
      [
        {
          name: "parent_1",
          attrs: [],
          children: [],
        },
        {
          name: "single_child",
          attrs: testFlatAttrs2,
          children: [],
        },
      ],
      [
        {
          name: "parent_2",
          attrs: [],
          children: [],
        },
      ],
    ],
  },
];

const treePlanWithChildren2: IExplainTreePlanNode = {
  name: "root",
  attrs: null,
  children: [
    {
      name: "single_grandparent",
      attrs: null,
      children: [
        {
          name: "single_parent",
          attrs: null,
          children: [
            {
              name: "child_1",
              attrs: testAttrs1,
              children: [],
            },
            {
              name: "child_2",
              attrs: testAttrs2,
              children: [],
            },
          ],
        },
      ],
    },
  ],
};

const expectedFlatPlanWithChildren2: FlatPlanNode[] = [
  {
    name: "root",
    attrs: [],
    children: [],
  },
  {
    name: "single_grandparent",
    attrs: [],
    children: [],
  },
  {
    name: "single_parent",
    attrs: [],
    children: [
      [
        {
          name: "child_1",
          attrs: testFlatAttrs1,
          children: [],
        },
      ],
      [
        {
          name: "child_2",
          attrs: testFlatAttrs2,
          children: [],
        },
      ],
    ],
  },
];

const treePlanWithNoChildren: IExplainTreePlanNode = {
  name: "root",
  attrs: testAttrs1,
  children: [],
};

const expectedFlatPlanWithNoChildren: FlatPlanNode[] = [
  {
    name: "root",
    attrs: testFlatAttrs1,
    children: [],
  },
];

describe("flattenTree", () => {
  describe("when node has children", () => {
    it("flattens single child paths.", () => {
      assert.deepEqual(
        flattenTree(treePlanWithSingleChildPaths),
        expectedFlatPlanWithSingleChildPaths,
      );
    });
    it("increases level if multiple children.", () => {
      assert.deepEqual(
        flattenTree(treePlanWithChildren1),
        expectedFlatPlanWithChildren1,
      );
      assert.deepEqual(
        flattenTree(treePlanWithChildren2),
        expectedFlatPlanWithChildren2,
      );
    });
  });
  describe("when node has no children", () => {
    it("returns valid flattened plan.", () => {
      assert.deepEqual(
        flattenTree(treePlanWithNoChildren),
        expectedFlatPlanWithNoChildren,
      );
    });
  });
});

describe("flattenAttributes", () => {
  describe("when all attributes have different keys", () => {
    it("creates array with exactly one value for each attribute", () => {
      const testAttrs: IAttr[] = [
        {
          key: "key1",
          value: "value1",
        },
        {
          key: "key2",
          value: "value2",
        },
      ];
      const expectedTestAttrs: FlatPlanNodeAttribute[] = [
        {
          key: "key1",
          values: ["value1"],
          warn: false,
        },
        {
          key: "key2",
          values: ["value2"],
          warn: false,
        },
      ];

      assert.deepEqual(flattenAttributes(testAttrs), expectedTestAttrs);
    });
  });
  describe("when there are multiple attributes with same key", () => {
    it("collects values into one array for same key", () => {
      const testAttrs: IAttr[] = [
        {
          key: "key1",
          value: "key1-value1",
        },
        {
          key: "key2",
          value: "key2-value1",
        },
        {
          key: "key1",
          value: "key1-value2",
        },
      ];
      const expectedTestAttrs: FlatPlanNodeAttribute[] = [
        {
          key: "key1",
          values: ["key1-value1", "key1-value2"],
          warn: false,
        },
        {
          key: "key2",
          values: ["key2-value1"],
          warn: false,
        },
      ];

      assert.deepEqual(flattenAttributes(testAttrs), expectedTestAttrs);
    });
  });
  describe("when attribute key/value is `spans FULL SCAN`", () => {
    it("sets warn to true", () => {
      const testAttrs: IAttr[] = [
        {
          key: "foo",
          value: "bar",
        },
        {
          key: "spans",
          value: "FULL SCAN",
        },
      ];
      const expectedTestAttrs: FlatPlanNodeAttribute[] = [
        {
          key: "foo",
          values: ["bar"],
          warn: false,
        },
        {
          key: "spans",
          values: ["FULL SCAN"],
          warn: true,
        },
      ];

      assert.deepEqual(flattenAttributes(testAttrs), expectedTestAttrs);
    });
  });
  describe("when keys are unsorted", () => {
    it("puts table key first, and sorts remaining keys alphabetically", () => {
      const testAttrs: IAttr[] = [
        {
          key: "zebra",
          value: "foo",
        },
        {
          key: "table",
          value: "foo",
        },
        {
          key: "cheetah",
          value: "foo",
        },
        {
          key: "table",
          value: "bar",
        },
      ];
      const expectedTestAttrs: FlatPlanNodeAttribute[] = [
        {
          key: "table",
          values: ["foo", "bar"],
          warn: false,
        },
        {
          key: "cheetah",
          values: ["foo"],
          warn: false,
        },
        {
          key: "zebra",
          values: ["foo"],
          warn: false,
        },
      ];

      assert.deepEqual(flattenAttributes(testAttrs), expectedTestAttrs);
    });
  });
});
