// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt
import React from "react";
import { Route, IndexRedirect } from "react-router";
import ClusterViz from "src/views/clusterviz/containers/map";
import NodeList from "src/views/clusterviz/containers/map/nodeList";
import ClusterOverview from "src/views/cluster/containers/clusterOverview";
import { normalizeConnectedComponent } from "src/util/normalizeConnectedComponent";

export const CLUSTERVIZ_ROOT = "/overview/map";

export default function createNodeMapRoutes(): JSX.Element {
  return (
    <Route path="overview" component={ ClusterOverview } >
      <IndexRedirect to="list" />
      <Route path="list" component={ NodeList } />
      <Route path="map(/**)" component={normalizeConnectedComponent(ClusterViz)} />
    </Route>
  );
}
