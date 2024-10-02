// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { useMemo } from "react";
import useSWR from "swr";

import { fetchData } from "src/api";
import { getRegionFromLocality } from "src/store/nodes";

import { NodeID, StoreID } from "../types/clusterTypes";

const NODES_PATH = "_status/nodes_ui";

export const getNodes =
  (): Promise<cockroach.server.serverpb.NodesResponse> => {
    return fetchData(cockroach.server.serverpb.NodesResponse, NODES_PATH);
  };

export const useNodeStatuses = () => {
  const { data, isLoading, error } = useSWR(NODES_PATH, getNodes, {
    revalidateOnFocus: false,
  });

  const { storeIDToNodeID, nodeIDToRegion } = useMemo(() => {
    const nodeIDToRegion: Record<NodeID, string> = {};
    const storeIDToNodeID: Record<StoreID, NodeID> = {};
    if (!data) {
      return { nodeIDToRegion, storeIDToNodeID };
    }
    data.nodes.forEach(ns => {
      ns.store_statuses.forEach(store => {
        storeIDToNodeID[store.desc.store_id as StoreID] = ns.desc
          .node_id as NodeID;
      });
      nodeIDToRegion[ns.desc.node_id as NodeID] = getRegionFromLocality(
        ns.desc.locality,
      );
    });
    return { nodeIDToRegion, storeIDToNodeID };
  }, [data]);

  return {
    data,
    isLoading,
    error,
    nodeIDToRegion,
    storeIDToNodeID,
  };
};
