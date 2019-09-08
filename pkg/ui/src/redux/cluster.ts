// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector, createStructuredSelector } from "reselect";
import { identity, truncate } from "lodash";

import { AdminUIState } from "src/redux/state";

export interface ClusterNameProps {
  clusterName: string;
  shortClusterName: string;
}

const getCachedData = createSelector(
  identity,
  (state: AdminUIState) => state.cachedData,
);
const getClusterData = createSelector(
  getCachedData,
  cachedData => cachedData.cluster.data,
);
const getClusterName = createSelector(
  getClusterData,
  clusterData => (clusterData) ? clusterData.cluster_name : "",
);
const getShortClusterName = createSelector(
  getClusterName,
  clusterName => truncate(clusterName),
);

export const clusterNameSelector = createStructuredSelector({
  clusterName: getClusterName,
  shortClusterName: getShortClusterName,
});
