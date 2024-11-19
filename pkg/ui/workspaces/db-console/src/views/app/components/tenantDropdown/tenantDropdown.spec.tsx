// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { shallow } from "enzyme";
import fetchMock from "fetch-mock";
import React from "react";

import { getCookieValue } from "src/redux/cookies";

import TenantDropdown from "./tenantDropdown";

jest.mock("src/redux/cookies", () => ({
  getCookieValue: jest.fn(),
}));

describe("TenantDropdown", () => {
  beforeEach(() => {});

  afterEach(() => {
    fetchMock.restore();
  });

  it("returns null if there's no current virtual cluster", () => {
    fetchMock.mock({
      matcher: `virtual_clusters`,
      method: "GET",
      response: () => {
        return {
          body: JSON.stringify({
            virtual_clusters: [],
          }),
        };
      },
    });

    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce(null);
    const wrapper = shallow(<TenantDropdown />);
    expect(wrapper.isEmptyRender());
  });
  // Multi-tenant scenarios
  it("returns null if there are no virtual clusters or less than 2 in the session cookie", () => {
    fetchMock.mock({
      matcher: `virtual_clusters`,
      method: "GET",
      response: () => {
        return {
          body: JSON.stringify({
            virtual_clusters: ["system"],
          }),
        };
      },
    });

    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("system");
    const wrapper = shallow(<TenantDropdown />);
    expect(wrapper.isEmptyRender());
  });
  it("returns a dropdown list of tenant options if there are multiple virtual clusters in the session cookie", () => {
    fetchMock.mock({
      matcher: `virtual_clusters`,
      method: "GET",
      response: () => {
        return {
          body: JSON.stringify({
            virtual_clusters: ["system", "app"],
          }),
        };
      },
    });

    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("system");
    const wrapper = shallow(<TenantDropdown />);
    expect(
      wrapper.find({ children: "Virtual cluster: system" }).length,
    ).toEqual(1);
  });
  it("returns a dropdown if the there is a single virtual cluster option but isn't system", () => {
    fetchMock.mock({
      matcher: `virtual_clusters`,
      method: "GET",
      response: () => {
        return {
          body: JSON.stringify({
            virtual_clusters: ["app"],
          }),
        };
      },
    });

    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("app");
    const wrapper = shallow(<TenantDropdown />);
    expect(wrapper.find({ children: "Virtual cluster: app" }).length).toEqual(
      1,
    );
  });
});
