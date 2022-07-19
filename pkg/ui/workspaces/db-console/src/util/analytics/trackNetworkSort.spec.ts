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
import { track } from "./trackNetworkSort";

describe("trackNetworkSort", () => {
  const sortBy = "Test";

  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(sortBy);
    expect(spy).toHaveBeenCalled();
  });

  it("should send the right event", () => {
    const spy = jest.fn();
    const expected = "Sort Network Diagnostics";

    track(spy)(sortBy);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(event === expected).toBe(true);
  });

  it("should send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(sortBy);

    const sent = spy.mock.calls[0][0];
    const sortedBy = get(sent, "properties.sortBy");

    expect(sortedBy === sortBy).toBe(true);
  });
});
