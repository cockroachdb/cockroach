// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import React from "react";

import { useNodeStatuses } from "src/api";
import { getRegionFromLocality } from "src/store/nodes";
import { StoreID } from "src/types/clusterTypes";

import { NodeRegionsSelector } from "./nodeRegionsSelector";

// Mock the useNodeStatuses hook
jest.mock("src/api", () => ({
  useNodeStatuses: jest.fn(),
}));

// Mock the getRegionFromLocality function
jest.mock("src/store/nodes", () => ({
  getRegionFromLocality: jest.fn(),
}));

const mockNodeData = {
  nodes: [
    {
      desc: { node_id: 1, locality: { region: "us-east" } },
      store_statuses: [
        { desc: { store_id: 101 } },
        { desc: { store_id: 102 } },
      ],
    },
    {
      desc: { node_id: 2, locality: { region: "us-west" } },
      store_statuses: [{ desc: { store_id: 201 } }],
    },
    {
      desc: { node_id: 3, locality: { region: "us-east" } },
      store_statuses: [{ desc: { store_id: 301 } }],
    },
  ],
};

describe("NodeRegionsSelector", () => {
  beforeEach(() => {
    (useNodeStatuses as jest.Mock).mockReturnValue({
      isLoading: false,
      data: mockNodeData,
    });
    (getRegionFromLocality as jest.Mock).mockImplementation(
      locality => locality.region,
    );
  });

  it("should render", () => {
    render(<NodeRegionsSelector value={[]} onChange={() => {}} />);
    expect(screen.getByText("Nodes")).toBeTruthy();
  });

  it("displays correct options based on node data", async () => {
    render(<NodeRegionsSelector value={[]} onChange={() => {}} />);

    const select = screen.getByText("Nodes");
    fireEvent.keyDown(select, { key: "ArrowDown" });

    await waitFor(() => {
      expect(screen.getByText("us-east")).toBeTruthy();
      expect(screen.getByText("us-west")).toBeTruthy();
      expect(screen.getByText("n1")).toBeTruthy();
      expect(screen.getByText("n2")).toBeTruthy();
      expect(screen.getByText("n3")).toBeTruthy();
    });
  });

  it("calls onChange with correct values when selecting options", async () => {
    const value: StoreID[] = [];
    const mockOnChange = jest.fn((selected: StoreID[]) => {
      value.push(...selected);
    });
    render(<NodeRegionsSelector value={value} onChange={mockOnChange} />);

    const select = screen.getByText("Nodes");
    fireEvent.keyDown(select, { key: "ArrowDown" });

    await waitFor(() => {
      fireEvent.click(screen.getByText("n1"));
    });

    expect(mockOnChange).toHaveBeenCalledWith([101, 102]);
  });

  it("displays selected values correctly", () => {
    render(
      <NodeRegionsSelector
        value={[101 as StoreID, 201 as StoreID]}
        onChange={() => {}}
      />,
    );

    expect(screen.getByText("n1")).toBeTruthy();
    expect(screen.getByText("n2")).toBeTruthy();
  });

  it("handles loading state", () => {
    (useNodeStatuses as jest.Mock).mockReturnValue({
      isLoading: true,
      data: mockNodeData,
    });

    render(<NodeRegionsSelector value={[]} onChange={() => {}} />);

    const select = screen.getByText("Nodes");
    fireEvent.keyDown(select, { key: "ArrowDown" });

    // In the loading state, the component should still render options
    // based on the existing data
    expect(screen.getByText("us-east")).toBeTruthy();
    expect(screen.getByText("us-west")).toBeTruthy();
    expect(screen.getByText("n1")).toBeTruthy();
    expect(screen.getByText("n2")).toBeTruthy();
    expect(screen.getByText("n3")).toBeTruthy();
  });
});
