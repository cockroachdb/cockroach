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
  it("returns null if there's no current virtual cluster", () => {
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
  it("returns null if there are no virtual clusters or less than 2 in the session cookie", () => {
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
  it("returns a dropdown list of tenant options if there are multiple virtual clusters in the session cookie", () => {
    (
      selectTenantsFromMultitenantSessionCookie as jest.MockedFn<
        typeof selectTenantsFromMultitenantSessionCookie
      >
    ).mockReturnValueOnce(["system", "app"]);
    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("system");
    const wrapper = shallow(<TenantDropdown />);
    expect(
      wrapper.find({ children: "Virtual cluster: system" }).length,
    ).toEqual(1);
  });
  it("returns a dropdown if the there is a single virtual cluster option but isn't system", () => {
    (
      selectTenantsFromMultitenantSessionCookie as jest.MockedFn<
        typeof selectTenantsFromMultitenantSessionCookie
      >
    ).mockReturnValueOnce(["app"]);
    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("app");
    const wrapper = shallow(<TenantDropdown />);
    expect(wrapper.find({ children: "Virtual cluster: app" }).length).toEqual(
      1,
    );
  });
});
