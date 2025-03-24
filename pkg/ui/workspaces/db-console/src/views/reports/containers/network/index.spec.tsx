// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { render, screen } from "@testing-library/react";
import { Location } from "history";
import Long from "long";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { Network } from "./index";

describe("Network", () => {
  const defaultProps = {
    nodesSummary: {
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
      nodeLastError: null as Error | null,
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
    },
    nodeSummaryErrors: [] as Error[],
    connectivity: {
      data: {
        connections: {
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
        },
        errors_by_node_id: {},
      },
      inFlight: false,
      valid: true,
      unauthorized: false,
    },
    refreshNodes: jest.fn(),
    refreshLiveness: jest.fn(),
    refreshConnectivity: jest.fn(),
    match: {
      params: {},
      isExact: true,
      path: "/network",
      url: "/network",
    },
    location: {
      pathname: "/network",
      search: "",
      state: null,
      hash: "",
    } as Location,
    history: {} as any,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renders network page with latency table", () => {
    render(
      <MemoryRouter>
        <Network {...defaultProps} />
      </MemoryRouter>,
    );

    expect(screen.getByText("Network")).toBeTruthy();
    expect(screen.getByRole("table")).toBeTruthy();
  });

  it("calls refresh functions on mount", () => {
    render(
      <MemoryRouter>
        <Network {...defaultProps} />
      </MemoryRouter>,
    );

    expect(defaultProps.refreshNodes).toHaveBeenCalled();
    expect(defaultProps.refreshLiveness).toHaveBeenCalled();
    expect(defaultProps.refreshConnectivity).toHaveBeenCalled();
  });

  it("displays message for single node cluster", () => {
    const propsWithSingleNode = {
      ...defaultProps,
      nodesSummary: {
        nodeStatuses: [defaultProps.nodesSummary.nodeStatuses[0]],
        nodeStatusByID: {
          "1": defaultProps.nodesSummary.nodeStatusByID["1"],
        },
        nodeIDs: ["1"],
        nodeDisplayNameByID: {
          "1": "node-1",
        },
        storeIDsByNodeID: {
          "1": ["1"],
        },
        nodeLastError: null as Error | null,
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
        },
        livenessStatusByNodeID: {
          "1": protos.cockroach.kv.kvserver.liveness.livenesspb
            .NodeLivenessStatus.NODE_STATUS_LIVE,
        },
      },
      connectivity: {
        data: {
          connections: {},
          errors_by_node_id: {},
        },
        inFlight: false,
        valid: true,
        unauthorized: false,
      },
    };

    render(
      <MemoryRouter>
        <Network {...propsWithSingleNode} />
      </MemoryRouter>,
    );

    expect(
      screen.getByText(
        "Cannot show latency chart for cluster with less than 2 nodes.",
      ),
    ).toBeTruthy();
  });

  it("handles peers with null latency", () => {
    const propsWithNullLatency = {
      ...defaultProps,
      connectivity: {
        ...defaultProps.connectivity,
        data: {
          connections: {
            "1": {
              peers: {
                "2": {
                  latency: {
                    nanos: 1000000,
                  } as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
                }, // 1ms
                "3": {
                  latency:
                    null as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
                }, // null latency
              },
            },
            "2": {
              peers: {
                "1": {
                  latency: {
                    nanos: 1000000,
                  } as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
                }, // 1ms
                "3": {
                  latency:
                    null as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
                }, // null latency
              },
            },
            "3": {
              peers: {
                "1": {
                  latency:
                    null as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
                }, // null latency
                "2": {
                  latency:
                    null as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
                }, // null latency
              },
            },
          },
          errors_by_node_id: {},
        },
        inFlight: false,
        valid: true,
        unauthorized: false,
      },
    };

    render(
      <MemoryRouter>
        <Network {...propsWithNullLatency} />
      </MemoryRouter>,
    );

    // Verify the component renders without errors
    expect(screen.getByText("Network")).toBeTruthy();
    expect(screen.getByRole("table")).toBeTruthy();
  });
});
