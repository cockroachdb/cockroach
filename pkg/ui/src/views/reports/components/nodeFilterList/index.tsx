import React from "react";
import _ from "lodash";

interface NodeFilterListProps {
  nodeIDs?: Set<number>;
  localityRegex?: RegExp;
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
