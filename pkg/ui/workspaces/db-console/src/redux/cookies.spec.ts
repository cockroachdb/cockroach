// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import {
  getAllCookies,
  getCookieValue,
  selectTenantsFromMultitenantSessionCookie,
  maybeClearTenantCookie,
  setCookie,
} from "./cookies";

describe("Cookies", () => {
  beforeEach(() => {
    Object.defineProperty(window.document, "cookie", {
      writable: true,
      value: "tenant=system;session=abc123,system,def456,demoapp",
    });
  });
  afterEach(() => {
    Object.defineProperty(window.document, "cookie", {
      writable: true,
      value: "",
    });
  });
  it("should return a map of cookie keys mapped to their values", () => {
    const result = getAllCookies();
    const expected = new Map();
    expected.set("tenant", "system");
    expected.set("session", "abc123,system,def456,demoapp");
    expect(result).toEqual(expected);
  });
  it("should return a cookie value by key or return null", () => {
    const result = getCookieValue("tenant");
    const expected = "system";
    expect(result).toEqual(expected);
    const result2 = getCookieValue("unknown");
    expect(result2).toBeNull();
  });
  it("should give a string array of tenants from the session cookie or an empty array", () => {
    const result = selectTenantsFromMultitenantSessionCookie();
    const expected = ["system", "demoapp"];
    expect(result).toEqual(expected);
    Object.defineProperty(window.document, "cookie", {
      writable: true,
      value: "",
    });
    const result2 = selectTenantsFromMultitenantSessionCookie();
    expect(result2).toEqual([]);
  });
  it("should clear the tenant cookie if in a multitenant cluster", () => {
    maybeClearTenantCookie();
    const tenantCookie = getCookieValue("tenant");
    expect(tenantCookie).toBeNull();
  });
  it("should set a cookie given a key and value", () => {
    setCookie("foo", "bar");
    const result = getCookieValue("foo");
    expect(result).toEqual("bar");
  });
});
