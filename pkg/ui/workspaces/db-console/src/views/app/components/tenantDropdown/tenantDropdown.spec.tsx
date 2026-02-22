// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, waitFor } from "@testing-library/react";
import fetchMock from "fetch-mock";
import React from "react";

import { getCookieValue } from "src/redux/cookies";

import TenantDropdown from "./tenantDropdown";

jest.mock("src/redux/cookies", () => ({
  ...jest.requireActual("src/redux/cookies"),
  getCookieValue: jest.fn(),
  setCookie: jest.fn(),
}));

describe("TenantDropdown", () => {
  afterEach(() => {
    fetchMock.restore();
  });

  const mockFetchClusters = (virtualClusters: string[]) => {
    fetchMock.mock({
      matcher: "virtual_clusters",
      method: "GET",
      response: () => ({
        body: JSON.stringify({ virtual_clusters: virtualClusters }),
      }),
    });
  };

  it("returns null if there's no current virtual cluster", async () => {
    mockFetchClusters([]);
    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce(null);

    const { container } = render(<TenantDropdown />);
    await waitFor(() => expect(fetchMock.called()).toBe(true));
    expect(container.innerHTML).toBe("");
  });

  it("returns null if there are no virtual clusters or less than 2 in the session cookie", async () => {
    mockFetchClusters(["system"]);
    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("system");

    const { container } = render(<TenantDropdown />);
    await waitFor(() => expect(fetchMock.called()).toBe(true));
    expect(container.innerHTML).toBe("");
  });

  it("renders a dropdown with the current virtual cluster when there are multiple options", async () => {
    mockFetchClusters(["system", "app"]);
    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("system");

    const { container } = render(<TenantDropdown />);
    await waitFor(() => {
      const selected = container.querySelector(".virtual-cluster-selected");
      expect(selected).not.toBeNull();
      expect(selected.textContent).toBe("Virtual cluster: system");
    });
  });

  it("renders a dropdown if there is a single virtual cluster option but isn't system", async () => {
    mockFetchClusters(["app"]);
    (
      getCookieValue as jest.MockedFn<typeof getCookieValue>
    ).mockReturnValueOnce("app");

    const { container } = render(<TenantDropdown />);
    await waitFor(() => {
      const selected = container.querySelector(".virtual-cluster-selected");
      expect(selected).not.toBeNull();
      expect(selected.textContent).toBe("Virtual cluster: app");
    });
  });
});
