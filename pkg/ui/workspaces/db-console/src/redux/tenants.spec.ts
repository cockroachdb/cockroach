// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { containsApplicationTenants } from "src/redux/tenants";

describe("containsApplicationTenants", () => {
  it("returns false on empty list", () => {
    expect(containsApplicationTenants([])).toEqual(false);
  });
  it("returns false with just a system tenant", () => {
    expect(
      containsApplicationTenants([
        {
          label: "system",
          value: "0",
        },
      ]),
    ).toEqual(false);
  });
  it("returns false with a system tenant and All", () => {
    expect(
      containsApplicationTenants([
        {
          label: "system",
          value: "0",
        },
        {
          label: "All",
          value: "",
        },
      ]),
    ).toEqual(false);
  });
  it("returns true with an app tenant", () => {
    expect(
      containsApplicationTenants([
        {
          label: "demo",
          value: "1",
        },
      ]),
    ).toEqual(true);
  });
  it("returns true with an app tenant and system and All", () => {
    expect(
      containsApplicationTenants([
        {
          label: "system",
          value: "0",
        },
        {
          label: "All",
          value: "",
        },
        {
          label: "demo",
          value: "1",
        },
      ]),
    ).toEqual(true);
  });
});
