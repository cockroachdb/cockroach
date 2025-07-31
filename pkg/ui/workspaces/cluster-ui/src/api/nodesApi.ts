// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { useContext, useMemo } from "react";

import { fetchData } from "src/api";
import { getRegionFromLocality } from "src/store/nodes";

import { ClusterDetailsContext } from "../contexts";
import { NodeID, StoreID } from "../types/clusterTypes";
import { useSwrWithClusterId } from "../util";

const NODES_PATH = "_status/nodes_ui";

export const getNodes =
  (): Promise<cockroach.server.serverpb.NodesResponse> => {
    return fetchData(cockroach.server.serverpb.NodesResponse, NODES_PATH);
  };

export type NodeStatus = {
  region: string;
  stores: StoreID[];
};

export const useNodeStatuses = () => {
  const { isTenant } = useContext(ClusterDetailsContext);
  const { data, isLoading, error } = useSwrWithClusterId(
    "nodesUI",
    !isTenant ? getNodes : null,
    {
      revalidateOnFocus: false,
      dedupingInterval: 10000, // 10 seconds.
    },
  );

  const { storeIDToNodeID, nodeStatusByID } = useMemo(() => {
    const nodeStatusByID: Record<NodeID, NodeStatus> = {};
    const storeIDToNodeID: Record<StoreID, NodeID> = {};
    if (!data) {
      return { nodeStatusByID, storeIDToNodeID };
    }
    data.nodes?.forEach(ns => {
      ns.store_statuses?.forEach(store => {
        storeIDToNodeID[store.desc.store_id as StoreID] = ns.desc
          .node_id as NodeID;
      });

      nodeStatusByID[ns.desc.node_id as NodeID] = {
        region: getRegionFromLocality(ns.desc.locality),
        stores: ns.store_statuses?.map(s => s.desc.store_id as StoreID),
      };
    });

    return { nodeStatusByID, storeIDToNodeID };
  }, [data]);

  return {
    isLoading,
    error,
    nodeStatusByID,
    storeIDToNodeID,
  };
};
