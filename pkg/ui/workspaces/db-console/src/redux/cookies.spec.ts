// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import {
  getAllCookies,
  getCookieValue,
  clearTenantCookie,
  setCookie,
} from "./cookies";

describe("Cookies", () => {
  beforeEach(() => {
    Object.defineProperty(window.document, "cookie", {
      writable: true,
      value: "tenant=system;someother=cookie;another=cookievalue",
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
    expected.set("someother", "cookie");
    expected.set("another", "cookievalue");
    expect(result).toEqual(expected);
  });
  it("should return a cookie value by key or return null", () => {
    const result = getCookieValue("tenant");
    const expected = "system";
    expect(result).toEqual(expected);
    const result2 = getCookieValue("unknown");
    expect(result2).toBeNull();
  });
  it("should clear the tenant cookie", () => {
    clearTenantCookie();
    const tenantCookie = getCookieValue("tenant");
    expect(tenantCookie).toBeNull();
  });
  it("should set a cookie given a key and value", () => {
    setCookie("foo", "bar");
    const result = getCookieValue("foo");
    expect(result).toEqual("bar");
  });
});
