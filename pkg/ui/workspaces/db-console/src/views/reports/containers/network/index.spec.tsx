// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useNodesSummary, useConnectivity } from "@cockroachlabs/cluster-ui";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { render, screen } from "@testing-library/react";
import Long from "long";
import React from "react";
import { MemoryRouter, Route } from "react-router-dom";

import Network from "./index";

jest.mock("@cockroachlabs/cluster-ui", () => {
  const actual = jest.requireActual("@cockroachlabs/cluster-ui");
  return {
    ...actual,
    useNodesSummary: jest.fn(),
    useConnectivity: jest.fn(),
  };
});

const mockUseNodesSummary = useNodesSummary as jest.Mock;
const mockUseConnectivity = useConnectivity as jest.Mock;

describe("Network", () => {
  const defaultNodesSummary = {
    nodeStatuses: [
      {
        desc: {
          node_id: 1,
          address: { address_field: "localhost:26257" },
          locality: {
            tiers: [
              { key: "region", value: "us-east-1" },
              { key: "zone", value: "a" },
            ],
          },
        },
        updated_at: Long.fromNumber(1000),
      },
      {
        desc: {
          node_id: 2,
          address: { address_field: "localhost:26258" },
          locality: {
            tiers: [
              { key: "region", value: "us-east-1" },
              { key: "zone", value: "b" },
            ],
          },
        },
        updated_at: Long.fromNumber(1000),
      },
    ] as any[],
    nodeStatusByID: {
      "1": {
        desc: {
          node_id: 1,
          address: { address_field: "localhost:26257" },
          locality: {
            tiers: [
              { key: "region", value: "us-east-1" },
              { key: "zone", value: "a" },
            ],
          },
        },
        updated_at: Long.fromNumber(1000),
      },
      "2": {
        desc: {
          node_id: 2,
          address: { address_field: "localhost:26258" },
          locality: {
            tiers: [
              { key: "region", value: "us-east-1" },
              { key: "zone", value: "b" },
            ],
          },
        },
        updated_at: Long.fromNumber(1000),
      },
    },
    nodeIDs: ["1", "2"],
    nodeDisplayNameByID: {
      "1": "node-1",
      "2": "node-2",
    },
    storeIDsByNodeID: {
      "1": ["1"],
      "2": ["2"],
    },
    livenessByNodeID: {
      "1": {
        nodeId: 1,
        epoch: Long.fromNumber(1),
        expiration: Long.fromNumber(1000),
        draining: false,
        membership:
          protos.cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus
            .ACTIVE,
      } as protos.cockroach.kv.kvserver.liveness.livenesspb.ILiveness,
      "2": {
        nodeId: 2,
        epoch: Long.fromNumber(1),
        expiration: Long.fromNumber(1000),
        draining: false,
        membership:
          protos.cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus
            .ACTIVE,
      } as protos.cockroach.kv.kvserver.liveness.livenesspb.ILiveness,
    },
    livenessStatusByNodeID: {
      "1": protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
        .NODE_STATUS_LIVE,
      "2": protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
        .NODE_STATUS_LIVE,
    },
    isLoading: false,
    error: undefined as Error | undefined,
  };

  const defaultConnections = {
    "1": {
      peers: {
        "2": { latency: { nanos: 1000000 } }, // 1ms
      },
    },
    "2": {
      peers: {
        "1": { latency: { nanos: 1000000 } }, // 1ms
      },
    },
  };

  beforeEach(() => {
    jest.clearAllMocks();
    mockUseNodesSummary.mockReturnValue(defaultNodesSummary);
    mockUseConnectivity.mockReturnValue({
      connections: defaultConnections,
      isLoading: false,
      error: undefined,
    });
  });

  it("renders network page with latency table", () => {
    render(
      <MemoryRouter initialEntries={["/network"]}>
        <Route path="/network">
          <Network />
        </Route>
      </MemoryRouter>,
    );

    expect(screen.getByText("Network")).toBeTruthy();
    expect(screen.getByRole("table")).toBeTruthy();
  });

  it("displays message for single node cluster", () => {
    mockUseNodesSummary.mockReturnValue({
      nodeStatuses: [defaultNodesSummary.nodeStatuses[0]],
      nodeStatusByID: {
        "1": defaultNodesSummary.nodeStatusByID["1"],
      },
      nodeIDs: ["1"],
      nodeDisplayNameByID: {
        "1": "node-1",
      },
      storeIDsByNodeID: {
        "1": ["1"],
      },
      livenessByNodeID: {
        "1": defaultNodesSummary.livenessByNodeID["1"],
      },
      livenessStatusByNodeID: {
        "1": protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
          .NODE_STATUS_LIVE,
      },
      isLoading: false,
      error: undefined,
    });
    mockUseConnectivity.mockReturnValue({
      connections: {},
      isLoading: false,
      error: undefined,
    });

    render(
      <MemoryRouter initialEntries={["/network"]}>
        <Route path="/network">
          <Network />
        </Route>
      </MemoryRouter>,
    );

    expect(
      screen.getByText(
        "Cannot show latency chart for cluster with less than 2 nodes.",
      ),
    ).toBeTruthy();
  });

  it("handles peers with null latency", () => {
    mockUseConnectivity.mockReturnValue({
      connections: {
        "1": {
          peers: {
            "2": {
              latency: { nanos: 1000000 },
            },
            "3": {
              latency: null,
            },
          },
        },
        "2": {
          peers: {
            "1": {
              latency: { nanos: 1000000 },
            },
            "3": {
              latency: null,
            },
          },
        },
        "3": {
          peers: {
            "1": {
              latency: null,
            },
            "2": {
              latency: null,
            },
          },
        },
      },
      isLoading: false,
      error: undefined,
    });

    render(
      <MemoryRouter initialEntries={["/network"]}>
        <Route path="/network">
          <Network />
        </Route>
      </MemoryRouter>,
    );

    // Verify the component renders without errors
    expect(screen.getByText("Network")).toBeTruthy();
    expect(screen.getByRole("table")).toBeTruthy();
  });
});
