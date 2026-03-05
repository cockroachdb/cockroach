// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { render, screen } from "@testing-library/react";
import Long from "long";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { Network } from "./index";

const twoNodeNodesResp = {
  nodes: [
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
      metrics: {},
      store_statuses: [] as any[],
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
      metrics: {},
      store_statuses: [] as any[],
      updated_at: Long.fromNumber(1000),
    },
  ],
};

const twoNodeLivenessResp = {
  livenesses: [
    {
      node_id: 1,
      epoch: Long.fromNumber(1),
      expiration: Long.fromNumber(1000),
      draining: false,
      membership:
        protos.cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus
          .ACTIVE,
    },
    {
      node_id: 2,
      epoch: Long.fromNumber(1),
      expiration: Long.fromNumber(1000),
      draining: false,
      membership:
        protos.cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus
          .ACTIVE,
    },
  ],
  statuses: {
    "1": protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
      .NODE_STATUS_LIVE,
    "2": protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
      .NODE_STATUS_LIVE,
  },
};

const twoNodeConnectivityResp = {
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
};

const defaultSwrResult = {
  isLoading: false,
  error: null as Error,
  mutate: jest.fn(),
  isValidating: false,
};

function mockSwrWithData(
  nodesData: any,
  livenessData: any,
  connectivityData: any,
) {
  return jest
    .spyOn(util, "useSwrWithClusterId")
    .mockImplementation((key: any) => {
      if (key === "networkNodes") {
        return { ...defaultSwrResult, data: nodesData };
      }
      if (key === "networkLiveness") {
        return { ...defaultSwrResult, data: livenessData };
      }
      if (key === "networkConnectivity") {
        return { ...defaultSwrResult, data: connectivityData };
      }
      return { ...defaultSwrResult, data: null };
    });
}

describe("Network", () => {
  let spy: jest.SpyInstance;

  afterEach(() => {
    spy.mockRestore();
  });

  it("renders network page with latency table", () => {
    spy = mockSwrWithData(
      twoNodeNodesResp,
      twoNodeLivenessResp,
      twoNodeConnectivityResp,
    );

    render(
      <MemoryRouter>
        <Network />
      </MemoryRouter>,
    );

    expect(screen.getByText("Network")).toBeTruthy();
    expect(screen.getByRole("table")).toBeTruthy();
  });

  it("fetches nodes, liveness, and connectivity data via SWR", () => {
    spy = mockSwrWithData(
      twoNodeNodesResp,
      twoNodeLivenessResp,
      twoNodeConnectivityResp,
    );

    render(
      <MemoryRouter>
        <Network />
      </MemoryRouter>,
    );

    // useSwrWithClusterId is called 3 times: nodes, liveness, connectivity.
    expect(spy).toHaveBeenCalledWith(
      "networkNodes",
      expect.any(Function),
      expect.any(Object),
    );
    expect(spy).toHaveBeenCalledWith(
      "networkLiveness",
      expect.any(Function),
      expect.any(Object),
    );
    expect(spy).toHaveBeenCalledWith(
      "networkConnectivity",
      expect.any(Function),
      expect.any(Object),
    );
  });

  it("displays message for single node cluster", () => {
    const singleNodeNodesResp = {
      nodes: [twoNodeNodesResp.nodes[0]],
    };
    const singleNodeLivenessResp = {
      livenesses: [twoNodeLivenessResp.livenesses[0]],
      statuses: {
        "1": protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
          .NODE_STATUS_LIVE,
      },
    };
    const singleNodeConnectivityResp = {
      connections: {},
      errors_by_node_id: {},
    };

    spy = mockSwrWithData(
      singleNodeNodesResp,
      singleNodeLivenessResp,
      singleNodeConnectivityResp,
    );

    render(
      <MemoryRouter>
        <Network />
      </MemoryRouter>,
    );

    expect(
      screen.getByText(
        "Cannot show latency chart for cluster with less than 2 nodes.",
      ),
    ).toBeTruthy();
  });

  it("handles peers with null latency", () => {
    const connectivityWithNulls = {
      connections: {
        "1": {
          peers: {
            "2": {
              latency: {
                nanos: 1000000,
              } as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
            },
            "3": {
              latency:
                null as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
            },
          },
        },
        "2": {
          peers: {
            "1": {
              latency: {
                nanos: 1000000,
              } as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
            },
            "3": {
              latency:
                null as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
            },
          },
        },
        "3": {
          peers: {
            "1": {
              latency:
                null as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
            },
            "2": {
              latency:
                null as protos.cockroach.server.serverpb.NetworkConnectivityResponse.IPeer["latency"],
            },
          },
        },
      },
      errors_by_node_id: {},
    };

    spy = mockSwrWithData(
      twoNodeNodesResp,
      twoNodeLivenessResp,
      connectivityWithNulls,
    );

    render(
      <MemoryRouter>
        <Network />
      </MemoryRouter>,
    );

    // Verify the component renders without errors
    expect(screen.getByText("Network")).toBeTruthy();
    expect(screen.getByRole("table")).toBeTruthy();
  });
});
