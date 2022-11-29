// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Route, Switch, useHistory, useRouteMatch } from "react-router-dom";
import React, { useEffect } from "react";
import SnapshotPage from "src/views/tracez_v2/snapshotPage";
import { join } from "path";
import { getDataFromServer } from "src/util/dataFromServer";

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
