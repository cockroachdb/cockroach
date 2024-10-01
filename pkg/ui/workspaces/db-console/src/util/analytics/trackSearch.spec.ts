// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import get from "lodash/get";
import isNumber from "lodash/isNumber";
import isString from "lodash/isString";

import { track } from "./trackSearch";

describe("trackSearch", () => {
  const testSearchResults = 3;

  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(testSearchResults);
    expect(spy).toHaveBeenCalled();
  });

  it("should send the right event", () => {
    const spy = jest.fn();
    const expected = "Search";

    track(spy)(testSearchResults);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(isString(event)).toBe(true);
    expect(event === expected).toBe(true);
  });

  it("should send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(testSearchResults);

    const sent = spy.mock.calls[0][0];
    const numberOfResults = get(sent, "properties.numberOfResults");

    expect(isNumber(numberOfResults)).toBe(true);
    expect(numberOfResults === testSearchResults).toBe(true);
  });
});
