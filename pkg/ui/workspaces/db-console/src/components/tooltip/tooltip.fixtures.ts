// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { NodeStatusRow } from "src/views/cluster/containers/nodesOverview";

export const nodeLocalityFixture: NodeStatusRow = {
  key: "-0",
  nodeId: 1,
  nodeName: "localhost:26257",
  uptime: "3 hours",
  replicas: 34,
  usedCapacity: 135351337,
  availableCapacity: 108590390313,
  usedMemory: 151085056,
  availableMemory: 8589934592,
  numCpus: 4,
  version: "v20.2.0-alpha.1-1355-ga0123f1bc0",
  status: 3,
  tiers: [
    {
      key: "region",
      value: "gcp-us-east1",
    },
  ],
  region: "gcp-us-east1",
};
