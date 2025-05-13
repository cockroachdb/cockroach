// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import {
  render,
  screen,
  fireEvent,
  waitFor,
  act,
} from "@testing-library/react";
import React from "react";

import * as api from "src/api/nodesApi";
import { NodeID, StoreID } from "src/types/clusterTypes";

import { NodeSelector } from "./nodeSelector";

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

describe("NodeSelector", () => {
  beforeEach(() => {
    // Mock the api.getNodes function at the module level
    jest.spyOn(api, "getNodes").mockResolvedValue(mockNodeData);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it("should render", () => {
    render(<NodeSelector initialValue={[]} onChange={() => {}} />);
    expect(screen.getByText("Select Nodes")).toBeTruthy();
  });

  it("displays correct options based on node data", async () => {
    render(<NodeSelector initialValue={[]} onChange={() => {}} />);

    // Find the select element by its name attribute and click it
    const select = screen.getByRole("combobox");

    await act(async () => {
      fireEvent.mouseDown(select);
    });

    await waitFor(() => {
      expect(screen.getByText("us-east")).toBeTruthy();
      expect(screen.getByText("us-west")).toBeTruthy();
      expect(screen.getByText("n1")).toBeTruthy();
      expect(screen.getByText("n2")).toBeTruthy();
      expect(screen.getByText("n3")).toBeTruthy();
    });
  });

  it("calls onSelect with correct values when selecting options", async () => {
    const mockOnChange = jest.fn();
    render(<NodeSelector initialValue={[]} onChange={mockOnChange} />);

    const select = screen.getByRole("combobox");

    await act(async () => {
      fireEvent.mouseDown(select);
    });

    await waitFor(async () => {
      const n1Option = screen.getByText("n1");
      await act(async () => {
        fireEvent.click(n1Option);
      });
    });

    // Click the Apply button to trigger onApply
    const applyButton = screen.getByText("Apply");
    await act(async () => {
      fireEvent.click(applyButton);
    });

    expect(mockOnChange).toHaveBeenCalledWith(
      [1],
      [{ id: 1, region: "us-east", stores: [101, 102] }],
    );
  });

  it("displays selected values correctly", () => {
    render(
      <NodeSelector
        initialValue={[1 as NodeID, 2 as NodeID]}
        onChange={() => {}}
      />,
    );

    expect(screen.getByText("n1")).toBeTruthy();
    expect(screen.getByText("n2")).toBeTruthy();
  });

  it("handles loading state", async () => {
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

    render(<NodeSelector initialValue={[]} onChange={() => {}} />);

    const select = screen.getByRole("combobox");

    await act(async () => {
      fireEvent.mouseDown(select);
    });

    // In the loading state, the component should still render options
    // based on the existing data
    await waitFor(() => {
      expect(screen.getByText("us-east")).toBeTruthy();
      expect(screen.getByText("us-west")).toBeTruthy();
      expect(screen.getByText("n1")).toBeTruthy();
      expect(screen.getByText("n2")).toBeTruthy();
      expect(screen.getByText("n3")).toBeTruthy();
    });
  });
});
