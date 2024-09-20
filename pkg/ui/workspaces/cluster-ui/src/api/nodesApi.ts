// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

  const nodeIDToRegion = useMemo(() => {
    const result: Record<NodeID, string> = {};
    if (!data) {
      return result;
    }
    data.nodes.forEach(ns => {
      result[ns.desc.node_id as NodeID] = getRegionFromLocality(
        ns.desc.locality,
      );
    });
    return result;
  }, [data]);

  const storeIDToNodeID = useMemo(() => {
    const result: Record<StoreID, NodeID> = {};
    if (!data) {
      return result;
    }
    data.nodes.forEach(ns => {
      ns.store_statuses.forEach(store => {
        result[store.desc.store_id as StoreID] = ns.desc.node_id as NodeID;
      });
    });
    return result;
  }, [data]);

  return {
    data,
    isLoading,
    error,
    nodeIDToRegion,
    storeIDToNodeID,
  };
};
