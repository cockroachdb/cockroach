// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import get from "lodash/get";
import isString from "lodash/isString";

import { track } from "./trackDownloadDiagnosticsBundle";

describe("trackDownloadDiagnosticsBundle", () => {
  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)("some statement");
    expect(spy).toHaveBeenCalled();
  });

  it("should send the right event", () => {
    const spy = jest.fn();
    const expected = "Diagnostics Bundle Download";

    track(spy)("whatever");

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(isString(event)).toBe(true);
    expect(event === expected).toBe(true);
  });

  it("should send the correct payload", () => {
    const spy = jest.fn();
    const statement = "SELECT blah from blah-blah";

    track(spy)(statement);

    const sent = spy.mock.calls[0][0];
    const fingerprint = get(sent, "properties.fingerprint");

    expect(isString(fingerprint)).toBe(true);
    expect(fingerprint === statement).toBe(true);
  });
});
