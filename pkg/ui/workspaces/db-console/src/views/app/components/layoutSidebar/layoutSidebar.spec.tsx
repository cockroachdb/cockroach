// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useNodes } from "@cockroachlabs/cluster-ui";
import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import Sidebar from "./index";

jest.mock("@cockroachlabs/cluster-ui", () => {
  const actual = jest.requireActual("@cockroachlabs/cluster-ui");
  return {
    ...actual,
    useNodes: jest.fn(),
  };
});

const mockUseNodes = useNodes as jest.Mock;

describe("LayoutSidebar", () => {
  const renderSidebar = () =>
    render(
      <MemoryRouter initialEntries={["/reports/network"]}>
        <Sidebar />
      </MemoryRouter>,
    );

  it("does not show Network link for single node cluster", () => {
    mockUseNodes.mockReturnValue({
      nodeStatuses: [{}],
      isLoading: false,
      error: undefined,
      nodeStatusByID: {},
      storeIDToNodeID: {},
      nodeRegionsByID: {},
    });
    renderSidebar();
    expect(screen.queryByText("Network")).toBeNull();
  });

  it("shows Network link for multi node cluster", () => {
    mockUseNodes.mockReturnValue({
      nodeStatuses: [{}, {}],
      isLoading: false,
      error: undefined,
      nodeStatusByID: {},
      storeIDToNodeID: {},
      nodeRegionsByID: {},
    });
    renderSidebar();
    expect(screen.getByText("Network")).toBeTruthy();
  });
});
