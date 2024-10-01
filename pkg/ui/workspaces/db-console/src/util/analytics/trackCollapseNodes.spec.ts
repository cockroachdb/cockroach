// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import get from "lodash/get";

import { track } from "./trackCollapseNodes";

describe("trackCollapseNodes", () => {
  const testCollapsed = true;

  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(testCollapsed);
    expect(spy).toHaveBeenCalled();
  });

  it("should send the right event", () => {
    const spy = jest.fn();
    const expected = "Collapse Nodes";

    track(spy)(testCollapsed);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(event === expected).toBe(true);
  });

  it("should send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(testCollapsed);

    const sent = spy.mock.calls[0][0];
    const collapsed = get(sent, "properties.collapsed");

    expect(collapsed === testCollapsed).toBe(true);
  });
});
