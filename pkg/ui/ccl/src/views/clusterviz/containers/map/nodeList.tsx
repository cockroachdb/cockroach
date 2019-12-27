// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import {InjectedRouter, RouterState} from "react-router";

import { NodesOverview } from "src/views/cluster/containers/nodesOverview";
import { Select } from "src/components/select";

import "./nodesList.styl";

const Option = Select.Option;

export default class NodeList extends React.Component<RouterState & { router: InjectedRouter }> {
  handleMapTableToggle = (value: string) => {
    this.props.router.push(`/overview/${value}`);
  }

  render() {
    // TODO(vilterp): dedup with ClusterVisualization
    return (
      <div className="fixed-panel">
        <div className="fixed-panel__panel-switcher">
          <Select
            defaultValue="list"
            display="link"
            onChange={this.handleMapTableToggle}
          >
            <Option value="list">Node List</Option>
            <Option value="map">Node Map</Option>
          </Select>
        </div>
        <div className="fixed-panel__content">
          <NodesOverview />
        </div>
      </div>
    );
  }
}
