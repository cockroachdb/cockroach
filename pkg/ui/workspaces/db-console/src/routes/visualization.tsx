// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Route, Switch, Redirect } from "react-router-dom";

import { NodesOverview } from "src/views/cluster/containers/nodesOverview";
import ClusterOverview from "src/views/cluster/containers/clusterOverview";

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
