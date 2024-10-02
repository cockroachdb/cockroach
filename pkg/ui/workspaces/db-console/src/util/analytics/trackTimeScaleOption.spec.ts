// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import get from "lodash/get";

import { track } from "./trackTimeScaleSelected";

describe("trackTimeScaleSelected", () => {
  const scale = "Last 2 weeks";

  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(scale);
    expect(spy).toHaveBeenCalled();
  });

  it("should send the right event", () => {
    const spy = jest.fn();
    const expected = "Time Scale Selected";

    track(spy)(scale);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(event === expected).toBe(true);
  });

  it("should send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(scale);

    const sent = spy.mock.calls[0][0];
    const timeScale = get(sent, "properties.timeScale");

    expect(timeScale === scale).toBe(true);
  });
});
