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
  queryToString,
  queryByName,
  queryToObj,
} from "./query";
import { Location } from "history";

const location: Location = {
  pathname: "/debug/chart",
  search: "?charts=%5B%7B%22axisUnits%22%3A0%2C%22metrics%22%3A%5B%7B%22downsampler%22%3A1%2C%22aggregator%22%3A2%2C%22derivative%22%3A0%2C%22perNode%22%3Afalse%2C%22source%22%3A%22%22%7D%5D%7D%2C%7B%22axisUnits%22%3A0%2C%22metrics%22%3A%5B%7B%22downsampler%22%3A1%2C%22aggregator%22%3A2%2C%22derivative%22%3A0%2C%22perNode%22%3Afalse%2C%22source%22%3A%22%22%7D%5D%7D%2C%7B%22axisUnits%22%3A0%2C%22metrics%22%3A%5B%7B%22downsampler%22%3A1%2C%22aggregator%22%3A2%2C%22derivative%22%3A0%2C%22perNode%22%3Afalse%2C%22source%22%3A%22%22%7D%5D%7D%5D&start=1579014600&end=1579101000",
  action: "POP",
  key: "",
  state: "",
  query: {
    charts: `[{"axisUnits":0,"metrics":[{"downsampler":1,"aggregator":2,"derivative":0,"perNode":false,"source":""}]},{"axisUnits":0,"metrics":[{"downsampler":1,"aggregator":2,"derivative":0,"perNode":false,"source":""}]},{"axisUnits":0,"metrics":[{"downsampler":1,"aggregator":2,"derivative":0,"perNode":false,"source":""}]}]`,
    end: "1579101000",
    start: "1579014600",
  },
};

describe("Query utils", () => {
  describe("queryToString", () => {
    it("make query to string", () => {
      assert.equal(queryToString({ a: "test" }), "a=test");
      assert.equal(queryToString({ a: "test", b: "test" }), "a=test&b=test");
      assert.equal(queryToString({ a: undefined }), "a=undefined");
    });
  });
  describe("queryByName", () => {
    it("get key from query", () => {
      assert.equal(queryByName(location, "start"), "1579014600");
      assert.equal(queryByName(location, "test"), null);
      assert.equal(queryByName(location, undefined), null);
    });
  });
  describe("queryToObj", () => {
    it("set/change value to/in query object", () => {
      assert.deepEqual(queryToObj(location, "start", "test"), { ...location.query, start: "test" });
      assert.deepEqual(queryToObj(location, undefined, "test"), location.query);
      assert.deepEqual(queryToObj(location, "test", "test"), { test: "test", ...location.query });
      assert.deepEqual(queryToObj(location, "test", undefined), location.query);
      assert.deepEqual(queryToObj(location, undefined, undefined), location.query);
    });
  });
});
