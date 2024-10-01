// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import get from "lodash/get";

import { track } from "./trackTimeFrameChange";

describe("trackPaginate", () => {
  const direction = "test";

  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(direction);
    expect(spy).toHaveBeenCalled();
  });

  it("should send the right event", () => {
    const spy = jest.fn();
    const expected = "Time Frame Change";

    track(spy)(direction);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(event === expected).toBe(true);
  });

  it("should send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(direction);

    const sent = spy.mock.calls[0][0];
    const changeDirection = get(sent, "properties.direction");

    expect(changeDirection === direction).toBe(true);
  });
});
