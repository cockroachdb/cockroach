// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { get, isString, isNumber } from "lodash";
import { assert } from "chai";
import { createSandbox } from "sinon";
import { track } from "./trackPaginate";

const sandbox = createSandbox();

describe("trackPaginate", () => {
  const testPage = 5;

  afterEach(() => {
    sandbox.reset();
  });

  it("should only call track once", () => {
    const spy = sandbox.spy();
    track(spy)(testPage);
    assert.isTrue(spy.calledOnce);
  });

  it("should send the right event", () => {
    const spy = sandbox.spy();
    const expected = "Paginate";

    track(spy)(testPage);

    const sent = spy.getCall(0).args[0];
    const event = get(sent, "event");

    assert.isTrue(isString(event));
    assert.isTrue(event === expected);
  });

  it("should send the correct payload", () => {
    const spy = sandbox.spy();

    track(spy)(testPage);

    const sent = spy.getCall(0).args[0];
    const selectedPage = get(sent, "properties.selectedPage");

    assert.isTrue(isNumber(selectedPage));
    assert.isTrue(selectedPage === testPage);
  });
});
