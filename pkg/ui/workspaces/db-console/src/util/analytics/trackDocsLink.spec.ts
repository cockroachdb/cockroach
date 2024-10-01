// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import get from "lodash/get";

import { track } from "./trackDocsLink";

describe("trackDocsLink", () => {
  const targetText = "Test target";

  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(targetText);
    expect(spy).toHaveBeenCalled();
  });

  it("should send the right event", () => {
    const spy = jest.fn();
    const expected = "Link to Docs";

    track(spy)(targetText);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(event === expected).toBe(true);
  });

  it("should send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(targetText);

    const sent = spy.mock.calls[0][0];
    const linkText = get(sent, "properties.linkText");

    expect(linkText === targetText).toBe(true);
  });
});
