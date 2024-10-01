// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import get from "lodash/get";
import isNumber from "lodash/isNumber";
import isString from "lodash/isString";

import { track } from "./trackPaginate";

describe("trackPaginate", () => {
  const testPage = 5;
  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(testPage);
    expect(spy).toHaveBeenCalled();
  });

  it("should send the right event", () => {
    const spy = jest.fn();
    const expected = "Paginate";

    track(spy)(testPage);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(isString(event)).toBe(true);
    expect(event === expected).toBe(true);
  });

  it("should send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(testPage);

    const sent = spy.mock.calls[0][0];
    const selectedPage = get(sent, "properties.selectedPage");

    expect(isNumber(selectedPage)).toBe(true);
    expect(selectedPage === testPage).toBe(true);
  });
});
