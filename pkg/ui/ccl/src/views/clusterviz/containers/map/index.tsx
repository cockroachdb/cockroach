// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import { RouterState } from "react-router";

import { Breadcrumbs } from "src/views/clusterviz/containers/map/breadcrumbs";
import NodeCanvasContainer from "src/views/clusterviz/containers/map/nodeCanvasContainer";
import TimeScaleDropdown from "src/views/cluster/containers/timescale";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { parseLocalityRoute } from "src/util/localities";
import "./sim.css";

export default class ClusterVisualization extends React.Component<RouterState> {
  render() {
    const tiers = parseLocalityRoute(this.props.params.splat);
    const options: DropdownOption[] = [
      { value: "map", label: "Node Map" },
      { value: "list", label: "Node List" },
    ];

    return (
      <div style={{
        width: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}>
        <div style={{
          flex: "none",
          backgroundColor: "white",
          boxShadow: "0px 4px 5px 0px lightgrey",
          // height: 70,
          zIndex: 5,
          // display: "flex",
          // justifyContent: "space-between",
          // alignItems: "center",
          padding: "16px 24px",
        }}>
          <div style={{ float: "left" }}><Dropdown title="View" selected="map" options={options} /></div>
          <div style={{ float: "right" }}><TimeScaleDropdown /></div>
          <div style={{ textAlign: "center", paddingTop: 13 }}><Breadcrumbs tiers={tiers} /></div>
        </div>
        <NodeCanvasContainer tiers={tiers} />
      </div>
    );
  }
}
