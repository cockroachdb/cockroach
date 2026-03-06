// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useNodesSummary } from "@cockroachlabs/cluster-ui";
import isNil from "lodash/isNil";
import { useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";

import { newerVersionsSelector } from "src/redux/alerts";
import { refreshVersion } from "src/redux/apiReducers";
import {
  clusterIdSelector,
  getSingleVersion,
  getVersions,
  validateNodes,
} from "src/redux/nodes";
import { AdminUIState, AppDispatch } from "src/redux/state";

/**
 * VersionCheckSync is a renderless component that dispatches
 * refreshVersion when the cluster ID and a single node version are
 * available. This replaces the version-check logic that was previously
 * in alertDataSync.
 */
function VersionCheckSync(): null {
  const dispatch: AppDispatch = useDispatch();
  const { nodeStatuses, livenessByNodeID } = useNodesSummary();
  const clusterId = useSelector((state: AdminUIState) =>
    clusterIdSelector(state),
  );
  const newerVersions = useSelector((state: AdminUIState) =>
    newerVersionsSelector(state),
  );

  useEffect(() => {
    if (isNil(newerVersions) && clusterId) {
      const validated = validateNodes(nodeStatuses, livenessByNodeID);
      const currentVersion = getSingleVersion(getVersions(validated));
      if (currentVersion) {
        dispatch(
          refreshVersion({
            clusterID: clusterId,
            buildtag: currentVersion,
          }),
        );
      }
    }
  }, [dispatch, nodeStatuses, livenessByNodeID, clusterId, newerVersions]);

  return null;
}

export default VersionCheckSync;
