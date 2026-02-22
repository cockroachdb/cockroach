// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import Sidebar from "./index";

const mockUseSelector = jest.fn();
jest.mock("react-redux", () => ({
  ...jest.requireActual("react-redux"),
  useSelector: (...args: any[]) => mockUseSelector(...args),
}));

describe("LayoutSidebar", () => {
  beforeEach(() => {
    mockUseSelector.mockReset();
  });

  const renderSidebar = () =>
    render(
      <MemoryRouter initialEntries={["/reports/network"]}>
        <Sidebar />
      </MemoryRouter>,
    );

  it("does not show Network link for single node cluster", () => {
    mockUseSelector.mockReturnValue(true);
    renderSidebar();
    expect(screen.queryByText("Network")).toBeNull();
  });

  it("shows Network link for multi node cluster", () => {
    mockUseSelector.mockReturnValue(false);
    renderSidebar();
    expect(screen.getByText("Network")).toBeTruthy();
  });
});
