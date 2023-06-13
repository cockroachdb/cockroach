// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import {
  selectTenantsFromMultitenantSessionCookie,
  getCookieValue,
} from "src/redux/cookies";
import React from "react";
import TenantDropdown from "./tenantDropdown";
import { shallow } from "enzyme";

jest.mock("src/redux/cookies", () => ({
  selectTenantsFromMultitenantSessionCookie: jest.fn(),
  getCookieValue: jest.fn(),
}));

describe("TenantDropdown", () => {
  it("returns null if there's no current tenant", () => {
    (
      selectTenantsFromMultitenantSessionCookie as jest.MockedFn<
        typeof selectTenantsFromMultitenantSessionCookie
      >
    ).mockReturnValueOnce([]);
    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce(null);
    const wrapper = shallow(<TenantDropdown />);
    expect(wrapper.isEmptyRender());
  });
  // Mutli-tenant scenarios
  it("returns null if there are no tenants or less than 2 tenants in the session cookie", () => {
    (
      selectTenantsFromMultitenantSessionCookie as jest.MockedFn<
        typeof selectTenantsFromMultitenantSessionCookie
      >
    ).mockReturnValueOnce(["system"]);
    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("system");
    const wrapper = shallow(<TenantDropdown />);
    expect(wrapper.isEmptyRender());
  });
  it("returns a dropdown list of tenant options if there are multiple tenant in the session cookie", () => {
    (
      selectTenantsFromMultitenantSessionCookie as jest.MockedFn<
        typeof selectTenantsFromMultitenantSessionCookie
      >
    ).mockReturnValueOnce(["system", "app"]);
    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("system");
    const wrapper = shallow(<TenantDropdown />);
    expect(wrapper.find({ children: "Tenant: system" }).length).toEqual(1);
  });
  it("returns a dropdown if the there is a single tenant option but isn't system tenant", () => {
    (
      selectTenantsFromMultitenantSessionCookie as jest.MockedFn<
        typeof selectTenantsFromMultitenantSessionCookie
      >
    ).mockReturnValueOnce(["app"]);
    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("app");
    const wrapper = shallow(<TenantDropdown />);
    expect(wrapper.find({ children: "Tenant: app" }).length).toEqual(1);
  });
});
