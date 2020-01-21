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
import { Dropdown } from "src/components/dropdown";
import { Button } from "src/components/button";

import "./nodesList.styl";

export default class NodeList extends React.Component<RouterState & { router: InjectedRouter }> {
  readonly items = [
    { value: "list", name: "Node List" },
    { value: "map", name: "Node Map" },
  ];

  handleMapTableToggle = (value: string) => {
    this.props.router.push(`/overview/${value}`);
  }

  render() {
    // TODO(vilterp): dedup with ClusterVisualization
    return (
      <div className="fixed-panel">
        <div className="fixed-panel__panel-switcher">
          <Dropdown
            items={this.items}
            onChange={this.handleMapTableToggle}
          >
            <Button type="flat" size="small">
              Node List
            </Button>
          </Dropdown>
        </div>
        <div className="fixed-panel__content">
          <NodesOverview />
        </div>
      </div>
    );
  }
}
