// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { get } from "lodash";
import { assert } from "chai";
import { createSandbox } from "sinon";
import { track } from "./trackStatementDetailsSubnavSelection";

const sandbox = createSandbox();

describe("trackSubnavSelection", () => {
  const subNavKey = "subnav-test";

  afterEach(() => {
    sandbox.reset();
  });

  it("should only call track once", () => {
    const spy = sandbox.spy();
    track(spy)(subNavKey);
    assert.isTrue(spy.calledOnce);
  });

  it("should send a track call with the correct event", () => {
    const spy = sandbox.spy();
    const expected = "SubNavigation Selection";

    track(spy)(subNavKey);

    const sent = spy.getCall(0).args[0];
    const event = get(sent, "event");

    assert.isTrue(event === expected);
  });

  it("send the correct payload", () => {
    const spy = sandbox.spy();

    track(spy)(subNavKey);

    const sent = spy.getCall(0).args[0];
    const selection = get(sent, "properties.selection");

    assert.isTrue(selection === subNavKey);
  });
});
