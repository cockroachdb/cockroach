// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import {InjectedRouter, RouteComponentProps, RouterState} from "react-router";

import { Breadcrumbs } from "src/views/clusterviz/containers/map/breadcrumbs";
import NodeCanvasContainer from "src/views/clusterviz/containers/map/nodeCanvasContainer";
import TimeScaleDropdown from "src/views/cluster/containers/timescale";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { NodesOverview } from "src/views/cluster/containers/nodesOverview";
import { parseLocalityRoute } from "src/util/localities";
import "./sim.css";

export default class NodeList extends React.Component<RouterState & { router: InjectedRouter }> {
  handleMapTableToggle = (opt: DropdownOption) => {
    this.props.router.push(`/overview/${opt.value}`);
  }

  render() {
    const tiers = parseLocalityRoute(this.props.params.splat);
    const options: DropdownOption[] = [
      { value: "map", label: "Node Map" },
      { value: "list", label: "Node List" },
    ];

    return (
      <div style={{
        width: "100%",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
        background: "white",
        paddingBottom: 24,
      }}>
        <div style={{
          flex: "none",
          backgroundColor: "white",
          boxShadow: "0 0 4px 0 rgba(0, 0, 0, 0.2)",
          zIndex: 5,
          padding: "16px 24px",
        }}>
          <div style={{ float: "left" }}>
            <Dropdown
              title="View"
              selected="list"
              options={options}
              onChange={this.handleMapTableToggle}
            />
          </div>
        </div>
        <NodesOverview />
      </div>
    );
  }
}
