// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import React from "react";

import * as api from "src/api/nodesApi";
import { NodeID, StoreID } from "src/types/clusterTypes";

import { NodeRegionsSelector } from "./nodeRegionsSelector";

import NodesResponse = cockroach.server.serverpb.NodesResponse;

const mockNodeData = new NodesResponse({
  nodes: [
    {
      desc: {
        node_id: 1,
        locality: { tiers: [{ key: "region", value: "us-east" }] },
      },
      store_statuses: [
        { desc: { store_id: 101 } },
        { desc: { store_id: 102 } },
      ],
    },
    {
      desc: {
        node_id: 2,
        locality: { tiers: [{ key: "region", value: "us-west" }] },
      },
      store_statuses: [{ desc: { store_id: 201 } }],
    },
    {
      desc: {
        node_id: 3,
        locality: { tiers: [{ key: "region", value: "us-east" }] },
      },
      store_statuses: [{ desc: { store_id: 301 } }],
    },
  ],
});

describe("NodeRegionsSelector", () => {
  beforeEach(() => {
    // Mock the api.getNodes function at the module level
    jest.spyOn(api, "getNodes").mockResolvedValue(mockNodeData);
  });

  afterEach(() => {
    jest.clearAllMocks();
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
    jest.spyOn(api, "useNodeStatuses").mockReturnValue({
      error: null,
      isLoading: true,
      nodeStatusByID: {
        1: { region: "us-east", stores: [101, 102] },
        2: { region: "us-west", stores: [201] },
        3: { region: "us-east", stores: [301] },
      } as Record<NodeID, api.NodeStatus>,
      storeIDToNodeID: {
        101: 1,
        102: 1,
        201: 2,
        301: 3,
      } as Record<StoreID, NodeID>,
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
