// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import groupBy from "lodash/groupBy";
import isEmpty from "lodash/isEmpty";
import mapValues from "lodash/mapValues";
import partition from "lodash/partition";
import { useMemo } from "react";

import { fetchData } from "src/api";

import { useSwrWithClusterId } from "../util";

type ILocation = cockroach.server.serverpb.LocationsResponse.ILocation;
type INodeStatus = cockroach.server.status.statuspb.INodeStatus;

const LOCATIONS_PATH = "_admin/v1/locations";
const LOCATIONS_SWR_KEY = "locations";

export interface LocationTree {
  [key: string]: {
    [value: string]: ILocation;
  };
}

const getLocations =
  (): Promise<cockroach.server.serverpb.LocationsResponse> => {
    return fetchData(
      cockroach.server.serverpb.LocationsResponse,
      LOCATIONS_PATH,
    );
  };

function nestLocations(data: ILocation[]): LocationTree {
  return data.reduce((acc: LocationTree, location: ILocation) => {
    const { locality_key: localityKey, locality_value: localityValue } =
      location;
    if (!acc[localityKey]) {
      acc[localityKey] = {};
    }
    acc[localityKey][localityValue] = location;
    return acc;
  }, {});
}

export interface LocalityTier {
  key: string;
  value: string;
}

export interface LocalityTree {
  tiers: LocalityTier[];
  localities: {
    [localityKey: string]: {
      [localityValue: string]: LocalityTree;
    };
  };
  nodes: INodeStatus[];
}

// buildLocalityTree constructs a hierarchical tree of localities from a list
// of node statuses. Nodes are grouped recursively by their locality tiers
// (e.g. region -> zone -> rack).
export function buildLocalityTree(
  nodes: INodeStatus[] = [],
  depth = 0,
): LocalityTree {
  const exceedsDepth = (node: INodeStatus) =>
    node.desc.locality.tiers.length > depth;
  const [subsequentNodes, thisLevelNodes] = partition(nodes, exceedsDepth);

  const localityKeyGroups = groupBy(
    subsequentNodes,
    node => node.desc.locality.tiers[depth].key,
  );

  const localityValueGroups = mapValues(
    localityKeyGroups,
    (group: INodeStatus[]) =>
      groupBy(group, node => node.desc.locality.tiers[depth].value),
  );

  const childLocalities = mapValues(localityValueGroups, groups =>
    mapValues(groups, (group: INodeStatus[]) =>
      buildLocalityTree(group, depth + 1),
    ),
  );

  const tiers = isEmpty(nodes)
    ? []
    : (nodes[0].desc.locality.tiers.slice(0, depth) as LocalityTier[]);

  return {
    tiers: tiers,
    nodes: thisLevelNodes,
    localities: childLocalities,
  };
}

export const useLocations = () => {
  const { data, isLoading, error } = useSwrWithClusterId(
    LOCATIONS_SWR_KEY,
    getLocations,
    {
      revalidateOnFocus: false,
      dedupingInterval: 600_000, // 10 minutes
    },
  );

  const locations = useMemo(() => data?.locations ?? [], [data]);

  const locationTree = useMemo(() => nestLocations(locations), [locations]);

  return {
    isLoading,
    error,
    locations,
    locationTree,
  };
};
