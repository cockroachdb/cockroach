// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { join } from "path";

import React, { useEffect } from "react";
import { Route, Switch, useHistory, useRouteMatch } from "react-router-dom";

import { getDataFromServer } from "src/util/dataFromServer";
import SnapshotPage from "src/views/tracez_v2/snapshotPage";

const NodePicker: React.FC = () => {
  // If no node was provided, navigate explicitly to the local node.
  const { url } = useRouteMatch();
  const history = useHistory();
  useEffect(() => {
    let targetNodeID = getDataFromServer().NodeID;
    if (!targetNodeID) {
      targetNodeID = "local";
    }
    history.location.pathname = join(url, "/node/", targetNodeID);
    history.replace(history.location);
  }, [url, history]);

  return null;
};

export const SnapshotRouter = () => {
  const { path } = useRouteMatch();
  return (
    <Switch>
      <Route exact path={path} component={NodePicker} />
      <Route
        exact
        path={join(path, "/node/:nodeID")}
        component={SnapshotPage}
      />
      <Route
        exact
        path={join(path, "/node/:nodeID/snapshot/:snapshotID")}
        component={SnapshotPage}
      />
      <Route
        exact
        path={join(path, "/node/:nodeID/snapshot/:snapshotID/span/:spanID")}
        component={SnapshotPage}
      />
      <Route
        exact
        path={join(path, "/node/:nodeID/snapshot/:snapshotID/span/:spanID/raw")}
        component={SnapshotPage}
      />
    </Switch>
  );
};
