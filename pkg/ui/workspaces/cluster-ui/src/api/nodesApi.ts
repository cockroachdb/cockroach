// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { useContext, useMemo } from "react";

import { fetchData } from "src/api";

import { ClusterDetailsContext } from "../contexts";
import { NodeID, StoreID } from "../types/clusterTypes";
import { useSwrWithClusterId } from "../util";

type INodeStatus = cockroach.server.status.statuspb.INodeStatus;
type ILocality = cockroach.roachpb.ILocality;

function getRegionFromLocality(locality: ILocality): string {
  for (let i = 0; i < locality.tiers.length; i++) {
    if (locality.tiers[i].key === "region") return locality.tiers[i].value;
  }
  return "";
}

const NODES_PATH = "_status/nodes_ui";

// SWR key for nodes data. Exported so other hooks (e.g. useNodesSummary)
// can share the same cache entry via SWR deduplication.
export const NODES_SWR_KEY = "nodesUI";

export const getNodes =
  (): Promise<cockroach.server.serverpb.NodesResponse> => {
    return fetchData(cockroach.server.serverpb.NodesResponse, NODES_PATH);
  };

export type NodeStatus = {
  id: NodeID;
  region: string;
  stores: StoreID[];
};

export const useNodes = () => {
  const { isTenant } = useContext(ClusterDetailsContext);
  const { data, isLoading, error } = useSwrWithClusterId(
    NODES_SWR_KEY,
    !isTenant ? getNodes : null,
    {
      revalidateOnFocus: false,
      dedupingInterval: 10000, // 10 seconds.
    },
  );

  const nodeStatuses: INodeStatus[] = data?.nodes ?? [];

  const { storeIDToNodeID, nodeStatusByID, nodeRegionsByID } = useMemo(() => {
    const nodeStatusByID: Record<NodeID, NodeStatus> = {};
    const storeIDToNodeID: Record<StoreID, NodeID> = {};
    const nodeRegionsByID: Record<string, string> = {};
    if (!data) {
      return { nodeStatusByID, storeIDToNodeID, nodeRegionsByID };
    }
    data.nodes?.forEach(ns => {
      ns.store_statuses?.forEach(store => {
        storeIDToNodeID[store.desc.store_id as StoreID] = ns.desc
          .node_id as NodeID;
      });

      const id = ns.desc.node_id as NodeID;
      const region = getRegionFromLocality(ns.desc.locality);
      nodeStatusByID[id] = {
        id,
        region,
        stores: ns.store_statuses?.map(s => s.desc.store_id as StoreID),
      };
      nodeRegionsByID[id.toString()] = region;
    });

    return { nodeStatusByID, storeIDToNodeID, nodeRegionsByID };
  }, [data]);

  return {
    isLoading,
    error,
    nodeStatuses,
    nodeStatusByID,
    storeIDToNodeID,
    nodeRegionsByID,
  };
};

/** @deprecated Use useNodes instead. */
export const useNodeStatuses = useNodes;
