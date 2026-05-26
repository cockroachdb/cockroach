// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import get from "lodash/get";

import { track } from "./trackFilter";

describe("trackFilter", () => {
  const filter = "Test";
  const filterValue = "test-value";

  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(filter, filterValue);
    expect(spy).toHaveBeenCalled();
  });

  it("should send a track call with the correct event", () => {
    const spy = jest.fn();
    const expected = "Test Filter";

    track(spy)(filter, filterValue);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(event === expected).toBe(true);
  });

  it("send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(filter, filterValue);

    const sent = spy.mock.calls[0][0];
    const selectedFilter = get(sent, "properties.selectedFilter");

    expect(selectedFilter === filterValue).toBe(true);
  });
});
