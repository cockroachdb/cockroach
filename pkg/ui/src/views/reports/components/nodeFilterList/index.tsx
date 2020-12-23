// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { Location } from "history";

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
  if (!_.isEmpty(nodeIds)) {
    const nodeIDs: Set<number> = new Set();
    _.forEach(_.split(nodeIds, ","), (nodeIDString) => {
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
  if (!_.isEmpty(locality)) {
    try {
      filters.localityRegex = new RegExp(locality);
    } catch (e) {
      // Ignore the error, the filter not appearing is feedback enough.
    }
  }

  return filters;
}

export function localityToString(locality: protos.cockroach.roachpb.ILocality) {
  return _.join(
    _.map(locality.tiers, (tier) => tier.key + "=" + tier.value),
    ",",
  );
}

export function NodeFilterList(props: NodeFilterListProps) {
  const { nodeIDs, localityRegex } = props;
  const filters: string[] = [];
  if (!_.isNil(nodeIDs) && nodeIDs.size > 0) {
    const nodeList = _.chain(Array.from(nodeIDs.keys()))
      .sort()
      .map((nodeID) => `n${nodeID}`)
      .join(",");
    filters.push(`Only nodes: ${nodeList}`);
  }
  if (!_.isNil(localityRegex)) {
    filters.push(`Locality Regex: ${localityRegex.source}`);
  }
  if (_.isEmpty(filters)) {
    return null;
  }

  return (
    <div>
      <h2 className="base-heading">Filters</h2>
      <ul className="node-filter-list">
        {_.map(filters, (filter, i) => (
          <li key={i}>{filter}</li>
        ))}
      </ul>
    </div>
  );
}
