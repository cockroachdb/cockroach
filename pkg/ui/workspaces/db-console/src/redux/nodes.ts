// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util, sumNodeStats } from "@cockroachlabs/cluster-ui";
import countBy from "lodash/countBy";

import * as protos from "src/js/protos";
import { cockroach } from "src/js/protos";
import { NoConnection } from "src/views/reports/containers/network";

import type { NodeSummaryStats } from "@cockroachlabs/cluster-ui";

/**
 * LivenessStatus is a type alias for the fully-qualified NodeLivenessStatus
 * enumeration. As an enum, it needs to be imported rather than using the 'type'
 * keyword.
 */
export import LivenessStatus = protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;

import MembershipStatus = cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;
import INodeStatus = cockroach.server.status.statuspb.INodeStatus;
import ILiveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;
import ILocality = cockroach.roachpb.ILocality;

const { MetricConstants } = util;

/**
 * livenessNomenclature resolves a mismatch between the terms used for liveness
 * status on our Admin UI and the terms used by the backend. Examples:
 * + "Live" on the server is "Healthy" on the Admin UI
 * + "Unavailable" on the server is "Suspect" on the Admin UI
 */
export function livenessNomenclature(liveness: LivenessStatus) {
  switch (liveness) {
    case LivenessStatus.NODE_STATUS_LIVE:
      return "healthy";
    case LivenessStatus.NODE_STATUS_UNAVAILABLE:
      return "suspect";
    case LivenessStatus.NODE_STATUS_DECOMMISSIONING:
      return "decommissioning";
    case LivenessStatus.NODE_STATUS_DECOMMISSIONED:
      return "decommissioned";
    case LivenessStatus.NODE_STATUS_DRAINING:
      return "draining";
    default:
      return "dead";
  }
}

export type { NodeSummaryStats };
export { sumNodeStats };

export interface CapacityStats {
  used: number;
  usable: number;
  available: number;
}

export function nodeCapacityStats(n: INodeStatus): CapacityStats {
  const used = n.metrics[MetricConstants.usedCapacity];
  const available = n.metrics[MetricConstants.availableCapacity];
  return {
    used,
    available,
    usable: used + available,
  };
}

export function getDisplayName(
  node: INodeStatus | NoConnection,
  livenessStatus = LivenessStatus.NODE_STATUS_LIVE,
  includeAddress = true,
) {
  const decommissionedString =
    livenessStatus === LivenessStatus.NODE_STATUS_DECOMMISSIONED
      ? "[decommissioned] "
      : "";

  if (isNoConnection(node)) {
    return `${decommissionedString}(n${node.from.nodeID})`;
  }
  if (includeAddress) {
    return `${decommissionedString}(n${node.desc.node_id}) ${node.desc.address.address_field}`;
  } else {
    return `${decommissionedString}n${node.desc.node_id}`;
  }
}

function isNoConnection(
  node: INodeStatus | NoConnection,
): node is NoConnection {
  return (
    (node as NoConnection).to !== undefined &&
    (node as NoConnection).from !== undefined
  );
}

export function getRegionFromLocality(locality: ILocality): string {
  for (let i = 0; i < locality.tiers.length; i++) {
    if (locality.tiers[i].key === "region") return locality.tiers[i].value;
  }
  return "";
}

// ---------------------------------------------------------------------------
// Pure helper functions (no Redux dependency). These are intended for use in
// components that fetch data via SWR hooks.
// ---------------------------------------------------------------------------

/**
 * getValidatedNodes filters node statuses to only include nodes that have build
 * info and whose membership status is ACTIVE (i.e. not decommissioning or
 * decommissioned).
 */
export function getValidatedNodes(
  nodeStatuses: INodeStatus[] | undefined,
  livenessByNodeID: Record<string, ILiveness>,
): INodeStatus[] | undefined {
  if (!nodeStatuses) {
    return undefined;
  }
  return nodeStatuses
    .filter(status => !!status.build_info)
    .filter(
      status =>
        !status.desc ||
        !livenessByNodeID[status.desc.node_id] ||
        !livenessByNodeID[status.desc.node_id].membership ||
        !(
          livenessByNodeID[status.desc.node_id].membership !==
          MembershipStatus.ACTIVE
        ),
    );
}

/**
 * getNumNodesByVersionsTag returns a map from build tag to the number of nodes
 * running that tag.
 */
export function getNumNodesByVersionsTag(
  validNodes: INodeStatus[] | undefined,
): Map<string, number> {
  if (!validNodes) {
    return new Map();
  }
  return new Map(
    Object.entries(countBy(validNodes, node => node?.build_info?.tag)),
  );
}

/**
 * getNumNodesByVersions returns a map from major.minor version string to the
 * number of nodes running that version.
 */
export function getNumNodesByVersions(
  validNodes: INodeStatus[] | undefined,
): Map<string, number> {
  if (!validNodes) {
    return new Map();
  }
  return new Map(
    Object.entries(
      countBy(validNodes, node => {
        const serverVersion = node?.desc?.ServerVersion;
        if (serverVersion) {
          return `${serverVersion.major_val}.${serverVersion.minor_val}`;
        }
        return "";
      }),
    ),
  );
}
