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
import { track } from "./trackStatementDetailsSubnavSelection";

const sandbox = createSandbox();

describe("trackSubnavSelection", () => {
  afterEach(() => {
    sandbox.reset();
  });

  it("should send a track call given a selected nav item key", () => {
    const spy = sandbox.spy();
    const subNavKey = "subnav-test";

    track(spy)(subNavKey);

    const sent = spy.getCall(0).args[0];
    const selection = get(sent, "properties.selection");

    assert.isTrue(spy.calledOnce);
    assert.isTrue(isString(selection));
    assert.isTrue(selection === subNavKey);
  });
});
