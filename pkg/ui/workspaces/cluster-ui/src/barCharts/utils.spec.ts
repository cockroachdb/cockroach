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
import { normalizeClosedDomain } from "./utils";

describe("barCharts utils", () => {
  describe("normalizeClosedDomain", () => {
    it("returns input args if domain values are not equal", () => {
      assert.deepStrictEqual(normalizeClosedDomain([10, 15]), [10, 15]);
    });

    it("returns increased end range by 1 if input start and end values are equal", () => {
      assert.deepStrictEqual(normalizeClosedDomain([10, 10]), [10, 11]);
    });
  });
});
