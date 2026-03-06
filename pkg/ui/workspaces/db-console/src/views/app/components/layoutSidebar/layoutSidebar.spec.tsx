// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import Sidebar from "./index";

const mockUseNodes = jest.fn();
jest.mock("@cockroachlabs/cluster-ui", () => ({
  ...jest.requireActual("@cockroachlabs/cluster-ui"),
  useNodes: () => mockUseNodes(),
}));

describe("LayoutSidebar", () => {
  beforeEach(() => {
    mockUseNodes.mockReset();
  });

  const renderSidebar = () =>
    render(
      <MemoryRouter initialEntries={["/reports/network"]}>
        <Sidebar />
      </MemoryRouter>,
    );

  it("does not show Network link for single node cluster", () => {
    mockUseNodes.mockReturnValue({
      nodeStatuses: [{ desc: { node_id: 1 } }],
    });
    renderSidebar();
    expect(screen.queryByText("Network")).toBeNull();
  });

  it("shows Network link for multi node cluster", () => {
    mockUseNodes.mockReturnValue({
      nodeStatuses: [
        { desc: { node_id: 1 } },
        { desc: { node_id: 2 } },
      ],
    });
    renderSidebar();
    expect(screen.getByText("Network")).toBeTruthy();
  });
});
