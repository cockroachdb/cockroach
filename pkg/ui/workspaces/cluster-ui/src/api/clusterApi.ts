// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { useMemo } from "react";

import { fetchData } from "src/api";

import { useSwrWithClusterId } from "../util";

import { useLiveness } from "./livenessApi";
import { useNodes } from "./nodesApi";

type INodeStatus = cockroach.server.status.statuspb.INodeStatus;
const LivenessStatus =
  cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
const MembershipStatus =
  cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;
type ILiveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;

const CLUSTER_PATH = "_admin/v1/cluster";
const CLUSTER_SWR_KEY = "cluster";

const getCluster = (): Promise<cockroach.server.serverpb.ClusterResponse> => {
  return fetchData(cockroach.server.serverpb.ClusterResponse, CLUSTER_PATH);
};

export const useCluster = () => {
  const { data, error, isLoading } = useSwrWithClusterId(
    CLUSTER_SWR_KEY,
    getCluster,
    {
      revalidateOnFocus: false,
      dedupingInterval: 30_000, // 30 seconds; cluster info rarely changes.
    },
  );
  return {
    clusterId: data?.cluster_id,
    isLoading,
    error,
  };
};

// getClusterName returns the name of the cluster from live node statuses.
// It picks the first non-empty cluster_name from live nodes.
export function getClusterName(
  nodeStatuses: INodeStatus[],
  livenessStatusByNodeID: Record<string, number>,
): string | undefined {
  if (!nodeStatuses?.length || !Object.keys(livenessStatusByNodeID).length) {
    return undefined;
  }
  const liveNodes = nodeStatuses.filter(
    ns =>
      livenessStatusByNodeID[ns.desc?.node_id?.toString()] ===
      LivenessStatus.NODE_STATUS_LIVE,
  );
  const nodeWithName = liveNodes.find(
    ns => ns.desc?.cluster_name && ns.desc.cluster_name.length > 0,
  );
  return nodeWithName?.desc?.cluster_name;
}

// getClusterVersionLabel returns the build version label. If nodes are
// running mixed versions, the lowest version is returned with a
// " - Mixed Versions" suffix.
export function getClusterVersionLabel(
  nodeStatuses: INodeStatus[],
  livenessByNodeID: Record<string, ILiveness>,
): string | undefined {
  if (!nodeStatuses) {
    return undefined;
  }
  // Filter to active, non-decommissioning nodes with build info.
  const validNodes = nodeStatuses
    .filter(ns => !!ns.build_info)
    .filter(ns => {
      const l = livenessByNodeID[ns.desc?.node_id?.toString()];
      return (
        !ns.desc ||
        !l ||
        l.membership == null ||
        l.membership === MembershipStatus.ACTIVE
      );
    });

  const versions = [...new Set(validNodes.map(ns => ns.build_info.tag))];
  if (!versions.length) {
    return undefined;
  }
  if (versions.length > 1) {
    const sorted = [...versions].sort();
    return `${sorted[0]} - Mixed Versions`;
  }
  return versions[0];
}

export const useClusterLabel = () => {
  const {
    nodeStatuses,
    isLoading: nodesLoading,
    error: nodesError,
  } = useNodes();

  const {
    livenesses,
    statuses: livenessStatusByNodeID,
    isLoading: livenessLoading,
    error: livenessError,
  } = useLiveness();

  const isLoading = nodesLoading || livenessLoading;

  const livenessByNodeID: Record<string, ILiveness> = useMemo(() => {
    const result: Record<string, ILiveness> = {};
    for (const l of livenesses) {
      if (l.node_id != null) {
        result[l.node_id.toString()] = l;
      }
    }
    return result;
  }, [livenesses]);

  const clusterName = useMemo(
    () => getClusterName(nodeStatuses, livenessStatusByNodeID),
    [nodeStatuses, livenessStatusByNodeID],
  );

  const clusterVersion = useMemo(
    () => getClusterVersionLabel(nodeStatuses, livenessByNodeID),
    [nodeStatuses, livenessByNodeID],
  );

  return {
    clusterName,
    clusterVersion,
    isLoading,
    error: nodesError ?? livenessError,
  };
};
