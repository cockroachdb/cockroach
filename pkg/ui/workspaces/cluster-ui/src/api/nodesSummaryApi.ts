// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { useMemo } from "react";

import { getDisplayName } from "../nodes";

import { useLiveness } from "./livenessApi";
import { useNodes } from "./nodesApi";

type INodeStatus = cockroach.server.status.statuspb.INodeStatus;
type LivenessStatus =
  cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
type ILiveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;

export type NodesSummary = {
  nodeStatuses: INodeStatus[];
  nodeIDs: string[];
  nodeStatusByID: Record<string, INodeStatus>;
  nodeDisplayNameByID: Record<string, string>;
  livenessStatusByNodeID: Record<string, LivenessStatus>;
  livenessByNodeID: Record<string, ILiveness>;
  storeIDsByNodeID: Record<string, string[]>;
};

interface UseNodesSummaryOptions {
  // Polling interval in milliseconds. Defaults to no polling (undefined).
  refreshInterval?: number;
}

export const useNodesSummary = (opts?: UseNodesSummaryOptions) => {
  const {
    nodeStatuses,
    isLoading: nodesLoading,
    error: nodesError,
  } = useNodes({ refreshInterval: opts?.refreshInterval });

  const {
    livenesses,
    statuses: livenessStatusByNodeID,
    isLoading: livenessLoading,
    error: livenessError,
  } = useLiveness({ refreshInterval: opts?.refreshInterval });

  const isLoading = nodesLoading || livenessLoading;

  const summary: NodesSummary = useMemo(() => {
    const nodeIDs: string[] = [];
    const nodeStatusByID: Record<string, INodeStatus> = {};
    const nodeDisplayNameByID: Record<string, string> = {};
    const storeIDsByNodeID: Record<string, string[]> = {};

    const livenessByNodeID: Record<string, ILiveness> = {};
    for (const l of livenesses) {
      if (l.node_id != null) {
        livenessByNodeID[l.node_id.toString()] = l;
      }
    }

    for (const ns of nodeStatuses) {
      const id = ns.desc?.node_id?.toString();
      if (id == null) continue;

      nodeIDs.push(id);
      nodeStatusByID[id] = ns;
      nodeDisplayNameByID[id] = getDisplayName(ns, livenessStatusByNodeID[id]);
      storeIDsByNodeID[id] = (ns.store_statuses ?? []).map(ss =>
        ss.desc.store_id.toString(),
      );
    }

    return {
      nodeStatuses,
      nodeIDs,
      nodeStatusByID,
      nodeDisplayNameByID,
      livenessStatusByNodeID,
      livenessByNodeID,
      storeIDsByNodeID,
    };
  }, [nodeStatuses, livenesses, livenessStatusByNodeID]);

  return {
    ...summary,
    isLoading,
    error: nodesError ?? livenessError,
  };
};
