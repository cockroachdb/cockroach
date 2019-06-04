// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

import React from "react";
import { Route, IndexRedirect } from "react-router";

import { NodesOverview } from "src/views/cluster/containers/nodesOverview";
import ClusterOverview from "src/views/cluster/containers/clusterOverview";

class NodesWrapper extends React.Component<{}, {}> {
  render() {
    return (
      <div style={{
        paddingTop: 12,
        width: "100%",
        height: "100%",
        overflow: "auto",
      }}>
        <NodesOverview />
      </div>
    );
  }
}

export default function createClusterOverviewRoutes(): JSX.Element {
  return (
    <Route path="overview" component={ ClusterOverview } >
      <IndexRedirect to="list" />
      <Route path="list" component={ NodesWrapper } />
    </Route>
  );
}
