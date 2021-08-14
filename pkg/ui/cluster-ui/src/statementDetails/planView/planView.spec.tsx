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
  flattenTreeAttributes,
  flattenAttributes,
} from "./planView";
import IAttr = cockroach.sql.ExplainTreePlanNode.IAttr;

type IExplainTreePlanNode = cockroach.sql.IExplainTreePlanNode;

const testAttrsDuplicatedKeys: IAttr[] = [
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

const expectedTestAttrsFlattened: FlatPlanNodeAttribute[] = [
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

const testAttrsDistinctKeys: IAttr[] = [
  {
    key: "key1",
    value: "value1",
  },
  {
    key: "key2",
    value: "value2",
  },
];

const expectedTestAttrsDistinctKeys: FlatPlanNodeAttribute[] = [
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

describe("flattenTreeAttributes", () => {
  describe("when all nodes have attributes with different keys", () => {
    it("creates array with exactly one value for each attribute for each node", () => {
      const node: IExplainTreePlanNode = {
        name: "root",
        attrs: testAttrsDistinctKeys,
        children: [
          {
            name: "child",
            attrs: testAttrsDistinctKeys,
            children: [
              {
                name: "grandchild",
                attrs: testAttrsDistinctKeys,
                children: [],
              },
            ],
          },
        ],
      };

      const nodeFlattened: FlatPlanNode = {
        name: "root",
        attrs: expectedTestAttrsDistinctKeys,
        children: [
          {
            name: "child",
            attrs: expectedTestAttrsDistinctKeys,
            children: [
              {
                name: "grandchild",
                attrs: expectedTestAttrsDistinctKeys,
                children: [],
              },
            ],
          },
        ],
      };

      assert.deepEqual(flattenTreeAttributes(node), nodeFlattened);
    });
  });
  describe("when there are nodes with multiple attributes with the same key", () => {
    it("flattens attributes for each node having multiple attributes with the same key", () => {
      const node: IExplainTreePlanNode = {
        name: "root",
        attrs: testAttrsDuplicatedKeys,
        children: [
          {
            name: "child",
            attrs: testAttrsDuplicatedKeys,
            children: [
              {
                name: "grandchild",
                attrs: testAttrsDuplicatedKeys,
                children: [],
              },
            ],
          },
        ],
      };

      const nodeFlattened: FlatPlanNode = {
        name: "root",
        attrs: expectedTestAttrsFlattened,
        children: [
          {
            name: "child",
            attrs: expectedTestAttrsFlattened,
            children: [
              {
                name: "grandchild",
                attrs: expectedTestAttrsFlattened,
                children: [],
              },
            ],
          },
        ],
      };

      assert.deepEqual(flattenTreeAttributes(node), nodeFlattened);
    });
  });
});

describe("flattenAttributes", () => {
  describe("when all attributes have different keys", () => {
    it("creates array with exactly one value for each attribute", () => {
      assert.deepEqual(
        flattenAttributes(testAttrsDistinctKeys),
        expectedTestAttrsDistinctKeys,
      );
    });
  });
  describe("when there are multiple attributes with same key", () => {
    it("collects values into one array for same key", () => {
      assert.deepEqual(
        flattenAttributes(testAttrsDuplicatedKeys),
        expectedTestAttrsFlattened,
      );
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
