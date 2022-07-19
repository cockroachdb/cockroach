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
import { track } from "./trackFilter";

describe("trackFilter", () => {
  const filter = "Test";
  const filterValue = "test-value";

  it("should only call track once", () => {
    const spy = jest.fn();
    track(spy)(filter, filterValue);
    expect(spy).toHaveBeenCalled();
  });

  it("should send a track call with the correct event", () => {
    const spy = jest.fn();
    const expected = "Test Filter";

    track(spy)(filter, filterValue);

    const sent = spy.mock.calls[0][0];
    const event = get(sent, "event");

    expect(event === expected).toBe(true);
  });

  it("send the correct payload", () => {
    const spy = jest.fn();

    track(spy)(filter, filterValue);

    const sent = spy.mock.calls[0][0];
    const selectedFilter = get(sent, "properties.selectedFilter");

    expect(selectedFilter === filterValue).toBe(true);
  });
});
