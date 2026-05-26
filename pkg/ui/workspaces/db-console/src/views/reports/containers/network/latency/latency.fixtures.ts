// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment-timezone";

import { ILatencyProps } from ".";

import NodeLivenessStatus = cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
import IPeer = cockroach.server.serverpb.NetworkConnectivityResponse.IPeer;
import ConnectionStatus = cockroach.server.serverpb.NetworkConnectivityResponse.ConnectionStatus;

const makePeer = (params?: Partial<IPeer>): IPeer => {
  const peer: IPeer = {
    latency: { nanos: Math.random() * 1000000 },
    status: ConnectionStatus.ESTABLISHED,
    address: "127.0.0.1:26257",
    locality: { tiers: [{ key: "az", value: "us" }] },
    ...params,
  };
  return peer;
};

export const latencyFixture: ILatencyProps = {
  displayIdentities: [
    {
      nodeID: 1,
      livenessStatus: NodeLivenessStatus.NODE_STATUS_LIVE,
      connectivity: {
        peers: {
          "1": makePeer(),
          "2": makePeer({ status: ConnectionStatus.ERROR }),
          "3": makePeer({
            status: ConnectionStatus.ERROR,
            latency: { nanos: 0 },
          }),
          "4": makePeer(),
          "5": makePeer(),
        },
      },
      address: "127.0.0.1:26257",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:08.122Z"),
    },
    {
      livenessStatus: NodeLivenessStatus.NODE_STATUS_LIVE,
      connectivity: {
        peers: {
          "1": makePeer(),
          "2": makePeer(),
          "3": makePeer(),
          "4": makePeer({
            status: ConnectionStatus.ESTABLISHING,
          }),
          "5": makePeer({
            status: ConnectionStatus.ESTABLISHING,
            latency: { nanos: 0 },
          }),
        },
      },
      nodeID: 2,
      address: "127.0.0.1:26259",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:08.674Z"),
    },
    {
      livenessStatus: NodeLivenessStatus.NODE_STATUS_LIVE,
      connectivity: {
        peers: {
          "1": makePeer({ status: ConnectionStatus.UNKNOWN }),
          "2": makePeer({
            status: ConnectionStatus.UNKNOWN,
            latency: { nanos: 0 },
          }),
          "3": makePeer(),
          "4": makePeer(),
          "5": makePeer(),
        },
      },
      nodeID: 3,
      address: "127.0.0.1:26261",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:25:52.640Z"),
    },
    {
      livenessStatus: NodeLivenessStatus.NODE_STATUS_LIVE,
      connectivity: {
        peers: {
          "1": makePeer({ status: ConnectionStatus.UNKNOWN }),
          "2": makePeer({
            status: ConnectionStatus.UNKNOWN,
            latency: { nanos: 0 },
          }),
          "3": makePeer(),
          "4": makePeer(),
          "5": makePeer(),
        },
      },
      nodeID: 4,
      address: "127.0.0.1:26263",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:09.871Z"),
    },
    {
      livenessStatus: NodeLivenessStatus.NODE_STATUS_LIVE,
      connectivity: {
        peers: {
          "1": makePeer(),
          "2": makePeer(),
          "3": makePeer(),
          "4": makePeer(),
          "5": makePeer(),
        },
      },
      nodeID: 5,
      address: "127.0.0.1:26263",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:09.871Z"),
    },
  ],
  multipleHeader: true,
  collapsed: false,
  std: {
    stddev: 0.3755919319616704,
    stddevMinus2: 0.29658363607665916,
    stddevMinus1: 0.6721755680383296,
    stddevPlus1: 1.4233594319616705,
    stddevPlus2: 1.798951363923341,
  },
  node_id: "region",
};

export const latencyFixtureWithNodeStatuses: ILatencyProps = {
  displayIdentities: [
    {
      nodeID: 1,
      livenessStatus: NodeLivenessStatus.NODE_STATUS_LIVE,
      connectivity: {
        peers: {
          "1": makePeer(),
          "2": makePeer({ status: ConnectionStatus.ERROR }),
          "3": makePeer({
            status: ConnectionStatus.ERROR,
            latency: { nanos: 0 },
          }),
          "4": makePeer({ status: ConnectionStatus.ERROR }),
          "5": makePeer(),
        },
      },
      address: "127.0.0.1:26257",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:08.122Z"),
    },
    {
      livenessStatus: NodeLivenessStatus.NODE_STATUS_DEAD,
      connectivity: null,
      nodeID: 2,
      address: "127.0.0.1:26259",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:08.674Z"),
    },
    {
      livenessStatus: NodeLivenessStatus.NODE_STATUS_UNAVAILABLE,
      connectivity: null,
      nodeID: 3,
      address: "127.0.0.1:26261",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:25:52.640Z"),
    },
    {
      livenessStatus: NodeLivenessStatus.NODE_STATUS_DECOMMISSIONED,
      connectivity: null,
      nodeID: 4,
      address: "127.0.0.1:26263",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:09.871Z"),
    },
    {
      livenessStatus: NodeLivenessStatus.NODE_STATUS_DECOMMISSIONING,
      connectivity: {
        peers: {
          "1": makePeer(),
          "5": makePeer(),
        },
      },
      nodeID: 5,
      address: "127.0.0.1:26263",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:09.871Z"),
    },
  ],
  multipleHeader: true,
  collapsed: false,
  std: {
    stddev: 0.3755919319616704,
    stddevMinus2: 0.29658363607665916,
    stddevMinus1: 0.6721755680383296,
    stddevPlus1: 1.4233594319616705,
    stddevPlus2: 1.798951363923341,
  },
  node_id: "region",
};
