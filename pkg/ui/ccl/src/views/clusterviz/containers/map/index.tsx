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
import { parseLocalityRoute } from "src/util/localities";
import "./sim.css";

export default class ClusterVisualization extends React.Component<RouterState> {
  render() {
    const tiers = parseLocalityRoute(this.props.params.splat);

    return (
      <div style={{
        width: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
      }}>
        <Breadcrumbs tiers={tiers} />
        <NodeCanvasContainer tiers={tiers} />
      </div>
    );
  }
}
