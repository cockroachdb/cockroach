// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";
import { Location } from "history";

import { propsToQueryString, queryByName } from "./query";

const location: Location = {
  pathname: "/debug/chart",
  search:
    "?charts=%5B%7B%22metrics%22%3A%5B%7B%22downsampler%22%3A1%2C%22aggregator%22%3A2%2C%22derivative%22%3A0%2C%22perNode%22%3Afalse%2C%22source%22%3A%22%22%2C%22metric%22%3A%22cr.node.build.timestamp%22%7D%2C%7B%22downsampler%22%3A1%2C%22aggregator%22%3A2%2C%22derivative%22%3A0%2C%22perNode%22%3Afalse%2C%22source%22%3A%22%22%2C%22metric%22%3A%22cr.node.changefeed.poll_request_nanos-p50%22%7D%5D%2C%22axisUnits%22%3A0%7D%5D&start=1581478532&end=1581500132",
  hash: "",
  state: null,
  key: null,
};

describe("Query utils", () => {
  describe("propsToQueryString", () => {
    it("creates query string from object", () => {
      const obj = {
        start: 100,
        end: 200,
        strParam: "hello",
        bool: false,
      };
      const expected = "start=100&end=200&strParam=hello&bool=false";
      const res = propsToQueryString(obj);
      expect(res).toEqual(expected);
    });

    it("skips entries with nullish values", () => {
      const obj: { [key: string]: unknown } = {
        start: 100,
        end: 200,
        strParam: null,
        hello: undefined,
      };
      const expected = "start=100&end=200";
      const res = propsToQueryString(obj);
      expect(res).toEqual(expected);
    });
  });
  describe("queryByName", () => {
    it("get key from query", () => {
      assert.equal(queryByName(location, "start"), "1581478532");
      assert.equal(queryByName(location, "test"), null);
      assert.equal(queryByName(location, undefined), null);
    });
  });
});
