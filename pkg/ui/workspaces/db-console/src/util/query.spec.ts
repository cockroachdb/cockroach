// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Location } from "history";
import mapValues from "lodash/mapValues";
import toString from "lodash/toString";
import Long from "long";

import { propsToQueryString, queryByName } from "./query";

const location: Location = {
  pathname: "/debug/chart",
  search:
    "?charts=%5B%7B%22metrics%22%3A%5B%7B%22downsampler%22%3A1%2C%22aggregator%22%3A2%2C%22derivative%22%3A0%2C%22perNode%22%3Afalse%2C%22source%22%3A%22%22%2C%22metric%22%3A%22cr.node.build.timestamp%22%7D%2C%7B%22downsampler%22%3A1%2C%22aggregator%22%3A2%2C%22derivative%22%3A0%2C%22perNode%22%3Afalse%2C%22source%22%3A%22%22%2C%22metric%22%3A%22cr.node.changefeed.some_metric-p50%22%7D%5D%2C%22axisUnits%22%3A0%7D%5D&start=1581478532&end=1581500132",
  hash: "",
  state: null,
  key: null,
};

describe("Query utils", () => {
  describe("propsToQueryString", function () {
    interface PropBag {
      [k: string]: string;
    }

    // helper decoding function used to doublecheck querystring generation
    function decodeQueryString(qs: string): PropBag {
      return qs.split("&").reduce((memo: PropBag, v: string) => {
        const [key, value] = v.split("=");
        memo[decodeURIComponent(key)] = decodeURIComponent(value).replace(
          "%20",
          "+",
        );
        return memo;
      }, {});
    }

    it("creates an appropriate querystring", function () {
      const testValues: { [k: string]: any } = {
        a: "testa",
        b: "testb",
      };

      const querystring = propsToQueryString(testValues);

      expect(/a=testa/.test(querystring)).toBeTruthy();
      expect(/b=testb/.test(querystring)).toBeTruthy();
      expect(querystring.match(/=/g).length).toBe(2);
      expect(querystring.match(/&/g).length).toBe(1);
      expect(testValues).toEqual(decodeQueryString(querystring));
    });

    it("handles falsy values correctly", function () {
      const testValues: { [k: string]: any } = {
        // null and undefined should be ignored
        undefined: undefined,
        null: null,
        // other values should be added
        false: false,
        "": "",
        0: 0,
      };

      const querystring = propsToQueryString(testValues);

      expect(/false=false/.test(querystring)).toBeTruthy();
      expect(/0=0/.test(querystring)).toBeTruthy();
      expect(/([^A-Za-z]|^)=([^A-Za-z]|$)/.test(querystring)).toBeTruthy();
      expect(querystring.match(/=/g).length).toBe(3);
      expect(querystring.match(/&/g).length).toBe(2);
      expect(/undefined/.test(querystring)).toBeFalsy();
      expect(/null/.test(querystring)).toBeFalsy();
      expect({ false: "false", "": "", 0: "0" }).toEqual(
        decodeQueryString(querystring),
      );
    });

    it("handles special characters", function () {
      const key = "!@#$%^&*()=+-_\\|\"`'?/<>";
      const value = key.split("").reverse().join(""); // key reversed
      const testValues: { [k: string]: any } = {
        [key]: value,
      };

      const querystring = propsToQueryString(testValues);

      expect(
        querystring.match(/%/g).length > (key + value).match(/%/g).length,
      ).toBeTruthy();
      expect(testValues).toEqual(decodeQueryString(querystring));
    });

    it("handles non-string values", function () {
      const testValues: { [k: string]: any } = {
        boolean: true,
        number: 1,
        emptyObject: {},
        emptyArray: [],
        objectWithProps: { a: 1, b: 2 },
        arrayWithElts: [1, 2, 3],
        long: Long.fromNumber(1),
      };

      const querystring = propsToQueryString(testValues);
      expect(mapValues(testValues, toString)).toEqual(
        decodeQueryString(querystring),
      );
    });
  });
  describe("queryByName", () => {
    it("get key from query", () => {
      expect(queryByName(location, "start")).toEqual("1581478532");
      expect(queryByName(location, "test")).toEqual(null);
      expect(queryByName(location, undefined)).toEqual(null);
    });
  });
});
