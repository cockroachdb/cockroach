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

export function localityToString(locality: protos.cockroach.roachpb.Locality$Properties) {
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
