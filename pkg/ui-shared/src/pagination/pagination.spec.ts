// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import {
  paginationPageCount,
} from "./pagination";

const match = {
  path: "/test/:app",
  url: "/test/$ cockroach demo",
  isExact: true,
  params: {
    app: "$ cockroach demo",
  },
};

describe("Pagination", () => {
  describe("page count", () => {
    it("get page count label", () => {
      assert.equal(paginationPageCount({ pageSize: 20, current: 1, total: 200 }, "test"), "1-20 of 200 test");
      assert.equal(paginationPageCount({ pageSize: 20, current: 1, total: 7 }, "test"), "1-7 of 7 test");
      assert.equal(paginationPageCount({ pageSize: 20, current: 1, total: 0 }, "test"), "0 test");
      assert.equal(paginationPageCount({ pageSize: 10, current: 2, total: 15 }, "test"), "11-15 of 15 test");
      assert.equal(paginationPageCount({ pageSize: 20, current: 2, total: 300 }, "test", match, "app"), "21-40 of 300 results");
      assert.equal(paginationPageCount({ pageSize: 20, current: 1, total: 64 }, "test", match, "app", "test"), "1-20 of 64 results");
      assert.equal(paginationPageCount({ pageSize: 20, current: 1, total: 0 }, "test", match, "app", "search"), "0 results");
      assert.equal(paginationPageCount({ pageSize: 10, current: 2, total: 15 }, "test", match, "app", "search"), "11-15 of 15 results");
    });
  });
});
