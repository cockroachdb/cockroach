// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { get, isString } from "lodash";
import { track } from "./trackActivateDiagnostics";

describe("trackActivateDiagnostics", () => {
  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)("some statement");
    expect(spy).toHaveBeenCalled();
  });

  it("should send the right event", () => {
    const spy = jest.fn();
    const expected = "Diagnostics Activation";

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
