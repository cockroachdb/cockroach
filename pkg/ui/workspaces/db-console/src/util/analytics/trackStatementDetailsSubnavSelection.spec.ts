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
import { track } from "./trackStatementDetailsSubnavSelection";

describe("trackSubnavSelection", () => {
  const subNavKey = "subnav-test";

  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(subNavKey);
    expect(spy).toHaveBeenCalled();
  });

  it("should send a track call with the correct event", () => {
    const spy = jest.fn();
    const expected = "SubNavigation Selection";

    track(spy)(subNavKey);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(event === expected).toBe(true);
  });

  it("send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(subNavKey);

    const sent = spy.mock.calls[0][0];
    const selection = get(sent, "properties.selection");

    expect(selection === subNavKey).toBe(true);
  });
});
