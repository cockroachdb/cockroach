// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import { Location } from "history";
import React from "react";

import * as protos from "src/js/protos";

export interface NodeFilterListProps {
  nodeIDs?: Set<number>;
  localityRegex?: RegExp;
}

export function getFilters(location: Location) {
  const filters: NodeFilterListProps = {};

  // Node id list.
  if (!_.isEmpty(location.query.node_ids)) {
    const nodeIDs: Set<number> = new Set();
    _.forEach(_.split(location.query.node_ids, ","), nodeIDString => {
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
  if (!_.isEmpty(location.query.locality)) {
    try {
      filters.localityRegex = new RegExp(location.query.locality);
    } catch (e) {
      // Ignore the error, the filter not appearing is feedback enough.
    }
  }

  return filters;
}

export function localityToString(locality: protos.cockroach.roachpb.ILocality) {
  return _.join(_.map(locality.tiers, (tier) => tier.key + "=" + tier.value), ",");
}

export function NodeFilterList(props: NodeFilterListProps) {
  const { nodeIDs, localityRegex } = props;
  const filters: string[] = [];
  if (!_.isNil(nodeIDs) && nodeIDs.size > 0) {
    const nodeList = _.chain(Array.from(nodeIDs.keys()))
      .sort()
      .map(nodeID => `n${nodeID}`)
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
      <h2>Filters</h2>
      <ul className="node-filter-list">
        {
          _.map(filters, (filter, i) => (
            <li key={i}>{filter}</li>
          ))
        }
      </ul>
    </div>
  );
}
