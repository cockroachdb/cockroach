// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import { columnSorterFactory } from "./columnSorterFactory";

describe("columnSorterFactory", () => {
  type SomeValue = {
    field1?: string;
    field2?: number;
    field3?: number;
  };

  const valueA: SomeValue = {
    field1: "a",
    field2: 1,
    field3: 5,
  };

  const valueB: SomeValue = {
    field1: "b",
    field2: 2,
    field3: 20,
  };

  describe("when field passed as a value", () => {
    const sorter = columnSorterFactory<SomeValue>("field1");

    it("properly sort values", () => {
      assert.equal(sorter(valueA, valueB), -1);
      assert.equal(sorter(valueB, valueA), 1);
    });

    it("properly sort values if one field is undefined", () => {
      assert.equal(sorter(valueA, { ...valueB, field1: undefined }), 1);
      assert.equal(sorter({ ...valueB, field1: undefined }, valueA), -1);
    });

    it("properly sort values if both fields are undefined", () => {
      assert.equal(sorter({ ...valueA, field1: undefined }, { ...valueB, field1: undefined }), 0);
    });

    it("properly sort values if both fields are equal", () => {
      assert.equal(sorter(valueA, valueA), 0);
    });
  });

  describe("when field passed as a transformation function", () => {

    const transformFunc = (value: SomeValue) => {
      if (!value.field2 || !value.field3) {
        return undefined;
      }
      return value.field2 + value.field3;
    };

    const sorter = columnSorterFactory<SomeValue>(transformFunc);

    it("properly sort values", () => {
      assert.equal(sorter(valueA, valueB), -1);
      assert.equal(sorter(valueB, valueA), 1);
    });

    it("properly sort values if one field is undefined", () => {
      assert.equal(
        sorter(
          valueA,
          { ...valueB, field2: undefined },
          ),
        1,
      );
      assert.equal(
        sorter(
          { ...valueB, field2: undefined },
          valueA,
        ),
        -1,
      );
    });

    it("properly sort values if both fields are undefined", () => {
      assert.equal(
        sorter(
        { ...valueA, field2: undefined },
        { ...valueB, field2: undefined },
        ),
        0,
      );
    });

    it("properly sort values if both fields are equal", () => {
      assert.equal(sorter(valueA, valueA), 0);
    });
  });
});
