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
  selectTenantsFromCookies,
  selectCurrentTenantFromCookies,
} from "src/redux/tenantOptions";
import React from "react";
import TenantDropdown from "./tenantDropdown";
import { shallow } from "enzyme";

jest.mock("src/redux/tenantOptions", () => ({
  selectTenantsFromCookies: jest.fn(),
  selectCurrentTenantFromCookies: jest.fn(),
}));

describe("TenantDropdown", () => {
  it("returns null if there are no tenants in the session cookie", () => {
    (
      selectTenantsFromCookies as jest.MockedFn<typeof selectTenantsFromCookies>
    ).mockReturnValueOnce([]);
    const wrapper = shallow(<TenantDropdown />);
    expect(wrapper.isEmptyRender());
  });
  it("returns a dropdown list of tenant options if there are tenant in the session cookie", () => {
    (
      selectTenantsFromCookies as jest.MockedFn<typeof selectTenantsFromCookies>
    ).mockReturnValueOnce(["system", "app"]);
    (
      selectCurrentTenantFromCookies as jest.MockedFn<
        typeof selectCurrentTenantFromCookies
      >
    ).mockReturnValueOnce("system");
    const wrapper = shallow(<TenantDropdown />);
    expect(wrapper.find({ children: "Tenant system" }).length).toEqual(1);
  });
});
