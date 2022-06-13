// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { get, isString } from "lodash";
import { createSandbox } from "sinon";
import { track } from "./trackCancelDiagnosticsBundle";

const sandbox = createSandbox();

describe("trackCancelDiagnosticsBundle", () => {
  afterEach(() => {
    sandbox.reset();
  });

  it("should only call track once", () => {
    const spy = sandbox.spy();
    track(spy)("some statement");
    expect(spy.calledOnce).toBe(true);
  });

  it("should send the right event", () => {
    const spy = sandbox.spy();
    const expected = "Diagnostics Bundle Cancellation";

    track(spy)("whatever");

    const sent = spy.getCall(0).args[0];
    const event = get(sent, "event");

    expect(isString(event)).toBe(true);
    expect(event === expected).toBe(true);
  });

  it("should send the correct payload", () => {
    const spy = sandbox.spy();
    const statement = "SELECT blah from blah-blah";

    track(spy)(statement);

    const sent = spy.getCall(0).args[0];
    const fingerprint = get(sent, "properties.fingerprint");

    expect(isString(fingerprint)).toBe(true);
    expect(fingerprint === statement).toBe(true);
  });
});
