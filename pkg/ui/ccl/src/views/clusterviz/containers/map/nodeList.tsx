// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import {InjectedRouter, RouterState} from "react-router";

import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { NodesOverview } from "src/views/cluster/containers/nodesOverview";

import "./nodesList.styl";

export default class NodeList extends React.Component<RouterState & { router: InjectedRouter }> {
  handleMapTableToggle = (opt: DropdownOption) => {
    this.props.router.push(`/overview/${opt.value}`);
  }

  render() {
    const options: DropdownOption[] = [
      { value: "map", label: "Node Map" },
      { value: "list", label: "Node List" },
    ];

    // TODO(vilterp): dedup with ClusterVisualization
    return (
      <div className="clusterviz fixed-panel">
        <div className="fixed-panel__panel-switcher">
          <Dropdown
            title="View"
            selected="list"
            options={options}
            onChange={this.handleMapTableToggle}
          />
        </div>
        <div className="fixed-panel__content">
          <NodesOverview />
        </div>
      </div>
    );
  }
}
