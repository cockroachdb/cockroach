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
import { track } from "./trackTimeFrameChange";

const sandbox = createSandbox();

describe("trackPaginate", () => {
  const direction = "test";

  afterEach(() => {
    sandbox.reset();
  });

  it("should only call track once", () => {
    const spy = sandbox.spy();
    track(spy)(direction);
    assert.isTrue(spy.calledOnce);
  });

  it("should send the right event", () => {
    const spy = sandbox.spy();
    const expected = "Time Frame Change";

    track(spy)(direction);

    const sent = spy.getCall(0).args[0];
    const event = get(sent, "event");

    assert.isTrue(event === expected);
  });

  it("should send the correct payload", () => {
    const spy = sandbox.spy();

    track(spy)(direction);

    const sent = spy.getCall(0).args[0];
    const changeDirection = get(sent, "properties.direction");

    assert.isTrue(changeDirection === direction);
  });
});
