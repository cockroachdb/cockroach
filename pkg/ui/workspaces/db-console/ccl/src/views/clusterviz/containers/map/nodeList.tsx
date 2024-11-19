// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { RouteComponentProps } from "react-router-dom";

import { Dropdown } from "src/components/dropdown";
import { NodesOverview } from "src/views/cluster/containers/nodesOverview";

import "./nodesList.styl";

export default class NodeList extends React.Component<RouteComponentProps> {
  readonly items = [
    { value: "list", name: "Node List" },
    { value: "map", name: "Node Map" },
  ];

  handleMapTableToggle = (value: string) => {
    this.props.history.push(`/overview/${value}`);
  };

  render() {
    // TODO(vilterp): dedup with ClusterVisualization
    return (
      <div className="fixed-panel">
        <div className="fixed-panel__panel-switcher">
          <Dropdown items={this.items} onChange={this.handleMapTableToggle}>
            Node List
          </Dropdown>
        </div>
        <div className="fixed-panel__content">
          <NodesOverview />
        </div>
      </div>
    );
  }
}
