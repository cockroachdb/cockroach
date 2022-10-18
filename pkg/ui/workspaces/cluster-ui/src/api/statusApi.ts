// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "src/api";
import NodeRequestMessage = cockroach.server.serverpb.NodeRequest;
import NodeResponseMessage = cockroach.server.serverpb.NodeResponse;

const STATUS_PREFIX = "_status";

export function getNodeUI(
  nodeID: string, // String to support `local`
): Promise<NodeResponseMessage> {
  const req: NodeRequestMessage = NodeRequestMessage.create({
    node_id: nodeID,
  });
  return fetchData(
    NodeResponseMessage,
    `${STATUS_PREFIX}/nodes_ui/${nodeID}`,
    NodeRequestMessage,
    null,
    "5S",
  );
}
