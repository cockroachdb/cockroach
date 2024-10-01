// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Location } from "history";
import forEach from "lodash/forEach";
import isEmpty from "lodash/isEmpty";
import isNil from "lodash/isNil";
import join from "lodash/join";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import split from "lodash/split";
import React from "react";

import * as protos from "src/js/protos";

export interface NodeFilterListProps {
  nodeIDs?: Set<number>;
  localityRegex?: RegExp;
}

export function getFilters(location: Location) {
  const filters: NodeFilterListProps = {};
  const searchParams = new URLSearchParams(location.search);
  const nodeIds = searchParams.get("node_ids");
  const locality = searchParams.get("locality");

  // Node id list.
  if (!isEmpty(nodeIds)) {
    const nodeIDs: Set<number> = new Set();
    forEach(split(nodeIds, ","), nodeIDString => {
      const nodeID = parseInt(nodeIDString, 10);
      if (nodeID) {
        nodeIDs.add(nodeID);
      }
    });
    if (nodeIDs.size > 0) {
      filters.nodeIDs = nodeIDs;
    }
  }

  // Locality regex filter.
  if (!isEmpty(locality)) {
    try {
      filters.localityRegex = new RegExp(locality);
    } catch (e) {
      // Ignore the error, the filter not appearing is feedback enough.
    }
  }

  return filters;
}

export function localityToString(locality: protos.cockroach.roachpb.ILocality) {
  return join(
    map(locality.tiers, tier => tier.key + "=" + tier.value),
    ",",
  );
}

export function NodeFilterList(props: NodeFilterListProps) {
  const { nodeIDs, localityRegex } = props;
  const filters: string[] = [];
  if (!isNil(nodeIDs) && nodeIDs.size > 0) {
    const nodeList = sortBy(Array.from(nodeIDs.keys()))
      .map(nodeID => `n${nodeID}`)
      .join(",");
    filters.push(`Only nodes: ${nodeList}`);
  }
  if (!isNil(localityRegex)) {
    filters.push(`Locality Regex: ${localityRegex.source}`);
  }
  if (isEmpty(filters)) {
    return null;
  }

  return (
    <div>
      <h2 className="base-heading">Filters</h2>
      <ul className="node-filter-list">
        {map(filters, (filter, i) => (
          <li key={i}>{filter}</li>
        ))}
      </ul>
    </div>
  );
}
