// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { propsToQueryString } from "./fetchData";

describe("fetchData functions", () => {
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
      const obj = {
        start: 100,
        end: 200,
        strParam: null as any,
        hello: undefined as any,
      };
      const expected = "start=100&end=200";
      const res = propsToQueryString(obj);
      expect(res).toEqual(expected);
    });
  });
});
