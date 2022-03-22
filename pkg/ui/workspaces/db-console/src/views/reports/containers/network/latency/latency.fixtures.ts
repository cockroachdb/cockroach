// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import Long from "long";
import { ILatencyProps } from ".";

const node1 = {
  desc: {
    node_id: 1,
    locality: {
      tiers: [
        { key: "region", value: "local" },
        { key: "zone", value: "local" },
      ],
    },
  },
  activity: {
    "1": { incoming: 85125, outgoing: 204928, latency: Long.fromInt(843506) },
    "2": {
      incoming: 1641005,
      outgoing: 196537462,
      latency: Long.fromInt(12141),
    },
    "3": { incoming: 27851, outgoing: 15530093 },
    "4": {
      incoming: 4346803,
      outgoing: 180065134,
      latency: Long.fromInt(505076),
    },
  },
};
const node2 = {
  desc: {
    node_id: 2,
    locality: {
      tiers: [
        { key: "region", value: "local" },
        { key: "zone", value: "local" },
      ],
    },
  },
  activity: {
    "1": {
      incoming: 8408467,
      outgoing: 97930352,
      latency: Long.fromInt(1455817),
    },
    "2": {},
    "3": { incoming: 22800, outgoing: 13925347 },
    "4": {
      incoming: 1328435,
      outgoing: 63271505,
      latency: Long.fromInt(766402),
    },
  },
};
const node3 = {
  desc: {
    node_id: 3,
    locality: {
      tiers: [
        { key: "region", value: "local" },
        { key: "zone", value: "local" },
      ],
    },
  },
  activity: {
    "1": { incoming: 49177, outgoing: 173961 },
    "2": { incoming: 98747, outgoing: 80848 },
    "3": {},
    "4": { incoming: 18239, outgoing: 12407 },
  },
};
const node4 = {
  desc: {
    node_id: 4,
    locality: {
      tiers: [
        { key: "region", value: "local" },
        { key: "zone", value: "local" },
      ],
    },
  },

  activity: {
    "1": {
      incoming: 4917367,
      outgoing: 51102302,
      latency: Long.fromInt(1589900),
    },
    "2": {
      incoming: 2657214,
      outgoing: 24963000,
      latency: Long.fromInt(1807269),
    },
    "3": { incoming: 26480, outgoing: 251052 },
    "4": {},
  },
};

export const latencyFixture: ILatencyProps = {
  displayIdentities: [
    {
      nodeID: 1,
      address: "127.0.0.1:26257",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:08.122Z"),
    },
    {
      nodeID: 2,
      address: "127.0.0.1:26259",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:08.674Z"),
    },
    {
      nodeID: 3,
      address: "127.0.0.1:26261",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:25:52.640Z"),
    },
    {
      nodeID: 4,
      address: "127.0.0.1:26263",
      locality: "region=local,zone=local",
      updatedAt: moment("2020-05-04T16:26:09.871Z"),
    },
  ],
  staleIDs: new Set([3]),
  multipleHeader: true,
  collapsed: false,
  nodesSummary: {
    nodeStatuses: [node1, node2, node3, node4],
    nodeIDs: [1, 2, 3, 4],
    nodeStatusByID: {
      "1": node1,
      "2": node2,
      "3": node3,
      "4": node4,
    },
  },
  std: {
    stddev: 0.3755919319616704,
    stddevMinus2: 0.29658363607665916,
    stddevMinus1: 0.6721755680383296,
    stddevPlus1: 1.4233594319616705,
    stddevPlus2: 1.798951363923341,
  },
  node_id: "region",
};

const nodeNoLocality1 = {
  desc: {
    node_id: 1,
  },
  activity: {
    "1": { incoming: 85125, outgoing: 204928, latency: Long.fromInt(843506) },
    "2": {
      incoming: 1641005,
      outgoing: 196537462,
      latency: Long.fromInt(12141),
    },
    "3": { incoming: 27851, outgoing: 15530093 },
    "4": {
      incoming: 4346803,
      outgoing: 180065134,
      latency: Long.fromInt(505076),
    },
  },
};
const nodeNoLocality2 = {
  desc: {
    node_id: 2,
  },
  activity: {
    "1": {
      incoming: 8408467,
      outgoing: 97930352,
      latency: Long.fromInt(1455817),
    },
    "2": {},
    "3": { incoming: 22800, outgoing: 13925347 },
    "4": {
      incoming: 1328435,
      outgoing: 63271505,
      latency: Long.fromInt(766402),
    },
  },
};
const nodeNoLocality3 = {
  desc: {
    node_id: 3,
  },
  activity: {
    "1": { incoming: 49177, outgoing: 173961 },
    "2": { incoming: 98747, outgoing: 80848 },
    "3": {},
    "4": { incoming: 18239, outgoing: 12407 },
  },
};
const nodeNoLocality4 = {
  desc: {
    node_id: 4,
    locality: {
      tiers: [
        { key: "region", value: "local" },
        { key: "zone", value: "local" },
      ],
    },
  },

  activity: {
    "1": {
      incoming: 4917367,
      outgoing: 51102302,
      latency: Long.fromInt(1589900),
    },
    "2": {
      incoming: 2657214,
      outgoing: 24963000,
      latency: Long.fromInt(1807269),
    },
    "3": { incoming: 26480, outgoing: 251052 },
    "4": {},
  },
};

export const latencyFixtureNoLocality: ILatencyProps = {
  displayIdentities: [
    {
      nodeID: 1,
      address: "127.0.0.1:26257",
      updatedAt: moment("2020-05-04T16:26:08.122Z"),
    },
    {
      nodeID: 2,
      address: "127.0.0.1:26259",
      updatedAt: moment("2020-05-04T16:26:08.674Z"),
    },
    {
      nodeID: 3,
      address: "127.0.0.1:26261",
      updatedAt: moment("2020-05-04T16:25:52.640Z"),
    },
    {
      nodeID: 4,
      address: "127.0.0.1:26263",
      updatedAt: moment("2020-05-04T16:26:09.871Z"),
    },
  ],
  staleIDs: new Set([3]),
  multipleHeader: true,
  collapsed: false,
  nodesSummary: {
    nodeStatuses: [
      nodeNoLocality1,
      nodeNoLocality2,
      nodeNoLocality3,
      nodeNoLocality4,
    ],
    nodeIDs: [1, 2, 3, 4],
    nodeStatusByID: {
      "1": nodeNoLocality1,
      "2": nodeNoLocality2,
      "3": nodeNoLocality3,
      "4": nodeNoLocality4,
    },
  },
  std: {
    stddev: 0.3755919319616704,
    stddevMinus2: 0.29658363607665916,
    stddevMinus1: 0.6721755680383296,
    stddevPlus1: 1.4233594319616705,
    stddevPlus2: 1.798951363923341,
  },
  node_id: "region",
};
