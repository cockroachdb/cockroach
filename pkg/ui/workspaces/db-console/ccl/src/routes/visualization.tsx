// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import React from "react";
import { Redirect, Route, Switch } from "react-router-dom";

import ClusterOverview from "src/views/cluster/containers/clusterOverview";
import ClusterViz from "src/views/clusterviz/containers/map";
import NodeList from "src/views/clusterviz/containers/map/nodeList";

export const CLUSTERVIZ_ROOT = "/overview/map";

export default function createNodeMapRoutes(): JSX.Element {
  return (
    <Route path="/overview">
      <ClusterOverview>
        <Switch>
          <Redirect exact from="/overview" to="/overview/list" />
          <Route path="/overview/list" component={NodeList} />
          <Route path="/overview/map" component={ClusterViz} />
        </Switch>
      </ClusterOverview>
    </Route>
  );
}
