// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import get from "lodash/get";

import { track } from "./trackStatementDetailsSubnavSelection";

describe("trackSubnavSelection", () => {
  const subNavKey = "subnav-test";

  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(subNavKey);
    expect(spy).toHaveBeenCalled();
  });

  it("should send a track call with the correct event", () => {
    const spy = jest.fn();
    const expected = "SubNavigation Selection";

    track(spy)(subNavKey);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(event === expected).toBe(true);
  });

  it("send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(subNavKey);

    const sent = spy.mock.calls[0][0];
    const selection = get(sent, "properties.selection");

    expect(selection === subNavKey).toBe(true);
  });
});
