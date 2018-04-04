import React from "react";
import { Route, IndexRedirect } from "react-router";

import { NodesOverview } from "src/views/cluster/containers/nodesOverview";
import ClusterOverview from "src/views/cluster/containers/clusterOverview";

class NodesWrapper extends React.Component<{}, {}> {
  render() {
    return (
      <div style={{ marginTop: 12 }}>
        <NodesOverview />
      </div>
    );
  }
}

export default function(): JSX.Element {
  return (
    <Route path="overview" component={ ClusterOverview } >
      <IndexRedirect to="list" />
      <Route path="list" component={ NodesWrapper } />
    </Route>
  );
}
