// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import { assert } from "chai";

import { cockroach } from "src/js/protos";
import { FlatPlanNode, flattenTree, planNodeHeaderProps } from "src/views/statements/planView";
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
    attrs: null,
    children: [],
  },
  {
    name: "single_grandparent",
    attrs: testAttrs1,
    children: [],
  },
  {
    name: "single_parent",
    attrs: null,
    children: [],
  },
  {
    name: "single_child",
    attrs: testAttrs2,
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
    attrs: testAttrs1,
    children: [],
  },
  {
    name: "single_grandparent",
    attrs: testAttrs1,
    children: [
      [
        {
          name: "parent_1",
          attrs: null,
          children: [],
        },
        {
          name: "single_child",
          attrs: testAttrs2,
          children: [],
        },
      ],
      [
        {
          name: "parent_2",
          attrs: null,
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
    attrs: null,
    children: [],
  },
  {
    name: "single_grandparent",
    attrs: null,
    children: [],
  },
  {
    name: "single_parent",
    attrs: null,
    children: [
      [
        {
          name: "child_1",
          attrs: testAttrs1,
          children: [],
        },
      ],
      [
        {
          name: "child_2",
          attrs: testAttrs2,
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
    attrs: testAttrs1,
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

describe("planNodeHeaderProps", () => {
  describe("when node is join node", () => {
    it("prepends join type to title.", () => {
      const result = planNodeHeaderProps({
        name: "join",
        attrs: [
          {
            key: "foo",
            value: "bar",
          },
          {
            key: "type",
            value: "inner",
          },
          {
            key: "baz",
            value: "foo-baz",
          },
        ],
        children: [],
      });
      assert.deepEqual(
        result,
        {
          title: "inner join",
          subtitle: null,
          warn: false,
        },
      );
    });
  });
  describe("when node is scan node", () => {
    describe("if not both `spans ALL` and `table` attribute", () => {
      it("returns default header properties.", () => {
        const result1 = planNodeHeaderProps({
          name: "scan",
          attrs: [
            {
              key: "spans",
              value: "ALL",
            },
            {
              key: "but-not-table-key",
              value: "table-name@key-type",
            },
          ],
          children: [],
        });
        const result2 = planNodeHeaderProps({
          name: "scan",
          attrs: [
            {
              key: "spans",
              value: "but-not-ALL-value",
            },
            {
              key: "table",
              value: "table-name@key-type",
            },
          ],
          children: [],
        });
        assert.deepEqual(
          result1,
          {
            title: "scan",
            subtitle: null,
            warn: false,
          },
        );
        assert.deepEqual(
          result2,
          {
            title: "scan",
            subtitle: null,
            warn: false,
          },
        );

      });
    });
    describe("if both `spans ALL` and `table` attribute", () => {
      it("warns user of table scan.", () => {
        const result = planNodeHeaderProps({
          name: "scan",
          attrs: [
            {
              key: "foo",
              value: "bar",
            },
            {
              key: "spans",
              value: "ALL",
            },
            {
              key: "baz",
              value: "foo-baz",
            },
            {
              key: "table",
              value: "table-name@key-type",
            },
          ],
          children: [],
        });
        assert.deepEqual(
          result,
          {
            title: "table scan",
            subtitle: "table-name",
            warn: true,
          },
        );
      });
    });
  });
});
