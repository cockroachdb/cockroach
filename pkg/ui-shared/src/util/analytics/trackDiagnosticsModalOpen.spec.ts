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
import { assert } from "chai";
import { createSandbox } from "sinon";
import { track } from "./trackDiagnosticsModalOpen";

const sandbox = createSandbox();

describe("trackDiagnosticsModalOpen", () => {
  afterEach(() => {
    sandbox.reset();
  });

  it("should only call track once", () => {
    const spy = sandbox.spy();
    track(spy)("some statement");
    assert.isTrue(spy.calledOnce);
  });

  it("should send a track call with the correct event", () => {
    const spy = sandbox.spy();
    const expected = "Diagnostics Modal Open";

    track(spy)("some statement");

    const sent = spy.getCall(0).args[0];
    const event = get(sent, "event");

    assert.isTrue(isString(event));
    assert.isTrue(event === expected);
  });

  it("send the correct payload", () => {
    const spy = sandbox.spy();
    const statement = "SELECT blah from blah-blah";

    track(spy)(statement);

    const sent = spy.getCall(0).args[0];
    const fingerprint = get(sent, "properties.fingerprint");

    assert.isTrue(isString(fingerprint));
    assert.isTrue(fingerprint === statement);
  });
});
