// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
