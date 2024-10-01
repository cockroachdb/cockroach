// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Route, Switch, Redirect } from "react-router-dom";

import ClusterOverview from "src/views/cluster/containers/clusterOverview";
import { NodesOverview } from "src/views/cluster/containers/nodesOverview";

class NodesWrapper extends React.Component<{}, {}> {
  render() {
    return (
      <div
        style={{
          paddingTop: 12,
          width: "100%",
          height: "100%",
          overflow: "auto",
        }}
      >
        <NodesOverview />
      </div>
    );
  }
}

export default function createClusterOverviewRoutes(): JSX.Element {
  return (
    <Route path="/overview">
      <ClusterOverview>
        <Switch>
          <Redirect exact from="/overview" to="/overview/list" />
          <Route path="/overview/list" component={NodesWrapper} />
        </Switch>
      </ClusterOverview>
    </Route>
  );
}
