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
import { track } from "./trackStatusFilter";

const sandbox = createSandbox();

describe("trackSubnavSelection", () => {
  const filter = "test filter";

  afterEach(() => {
    sandbox.reset();
  });

  it("should only call track once", () => {
    const spy = sandbox.spy();
    track(spy)(filter);
    assert.isTrue(spy.calledOnce);
  });

  it("should send a track call with the correct event", () => {
    const spy = sandbox.spy();
    const expected = "Status Filter";

    track(spy)(filter);

    const sent = spy.getCall(0).args[0];
    const event = get(sent, "event");

    assert.isTrue(isString(event));
    assert.isTrue(event === expected);
  });

  it("send the correct payload", () => {
    const spy = sandbox.spy();

    track(spy)(filter);

    const sent = spy.getCall(0).args[0];
    const selectedFilter = get(sent, "properties.selectedFilter");

    assert.isTrue(isString(selectedFilter));
    assert.isTrue(selectedFilter === filter);
  });
});
