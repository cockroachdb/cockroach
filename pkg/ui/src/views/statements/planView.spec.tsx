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
import { collapseRepeatedAttrs } from "src/views/statements/planView";
import ExplainTreePlanNode_IAttr = cockroach.sql.ExplainTreePlanNode.IAttr;

describe("collapseRepeatedAttrs", () => {

  describe("when attributes array is empty", () => {
    it("returns an empty result array.", () => {
      const result = collapseRepeatedAttrs(makeAttrs([
      ]));
      assert.deepEqual(result, [
      ]);
    });
  });

  describe("when attributes contain null properties", () => {
    it("ignores attributes with null properties.", () => {
      const result = collapseRepeatedAttrs([
        {
          key: null,
          value: null,
        },
        {
          key: "key",
          value: null,
        },
        {
          key: "key",
          value: "value",
        },
        {
          key: null,
          value: "value",
        },
      ]);
      assert.deepEqual(result, [
        {
          key: "key",
          value: ["value"],
        },
      ]);
    });
  });

  describe("when attributes contains duplicate keys", () => {
    it("groups values with same key.", () => {
      const result = collapseRepeatedAttrs(makeAttrs([
        "key1: value1",
        "key2: value1",
        "key1: value2",
        "key2: value2",
        "key1: value3",
      ]));
      assert.deepEqual(result, [
        {
          key: "key1",
          value: ["value1", "value2", "value3"],
        },
        {
          key: "key2",
          value: ["value1", "value2"],
        },
      ]);
    });
  });

  describe("when attribute keys are not alphabetized", () => {
    describe("when no table key present", () => {
      it("sorts attributes alphabetically by key.", () => {
        const result = collapseRepeatedAttrs(makeAttrs([
          "papaya: papaya",
          "coconut: coconut",
          "banana: banana",
          "mango: mango",
        ]));
        assert.deepEqual(result, [
          {
            key: "banana",
            value: ["banana"],
          },
          {
            key: "coconut",
            value: ["coconut"],
          },
          {
            key: "mango",
            value: ["mango"],
          },
          {
            key: "papaya",
            value: ["papaya"],
          },
        ]);
      });
    });
    describe("when table key is present", () => {
      it("sorts attribute with table key first.", () => {
        const result = collapseRepeatedAttrs(makeAttrs([
          "papaya: papaya",
          "coconut: coconut",
          "banana: banana",
          "table: table",
          "mango: mango",
        ]));
        assert.deepEqual(result, [
          {
            key: "table",
            value: ["table"],
          },
          {
            key: "banana",
            value: ["banana"],
          },
          {
            key: "coconut",
            value: ["coconut"],
          },
          {
            key: "mango",
            value: ["mango"],
          },
          {
            key: "papaya",
            value: ["papaya"],
          },
        ]);
      });
    });
  });
});

function makeAttrs(attributes: string[]): ExplainTreePlanNode_IAttr[] {
  return attributes.map((attribute) => {
    return makeAttr(attribute.split(": "));
  });
}

function makeAttr(parts: string[]): ExplainTreePlanNode_IAttr {
  if (parts.length < 2) {
    return null;
  }
  return {
    key: parts[0],
    value: parts[1],
  };
}
