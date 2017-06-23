import React from "react";
import _ from "lodash";

export interface FilterListProps {
  nodeIDs?: Map<number, {}>;
  localityRegex?: RegExp;
}

export function FilterList(props: FilterListProps) {
  const filters: string[] = [];
  if (!_.isNil(props.nodeIDs) && props.nodeIDs.size > 0) {
    const nodeList = _.join(_.map(Array.from(props.nodeIDs.keys()).sort(), (nodeID) => `n${nodeID}`), ", ");
    filters.push(`Only nodes: ${nodeList}`);
  }
  if (!_.isNil(props.localityRegex)) {
    filters.push(`Locality Regex: ${props.localityRegex.source}`);
  }
  if (_.isEmpty(filters)) {
    return null;
  }
  return <div>
    <h2>Filters</h2>
    <ul className="filter-ul">
      {
        _.map(filters, (filter, i) => (
          <li key={i}>{filter}</li>
        ))
      }
    </ul>
  </div>;
}
