// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Loading, useNodesSummary } from "@cockroachlabs/cluster-ui";
import isNil from "lodash/isNil";
import React, { useEffect, useMemo } from "react";
import { connect } from "react-redux";
import { useHistory } from "react-router-dom";

import { refreshLocations } from "src/redux/apiReducers";
import { buildLocalityTree, LocalityTier } from "src/redux/localities";
import {
  selectLocationsRequestStatus,
  selectLocationTree,
  LocationTree,
} from "src/redux/locations";
import { LivenessStatus } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { getLocality } from "src/util/localities";

import { NodeCanvas } from "./nodeCanvas";

interface NodeCanvasContainerReduxProps {
  locationTree: LocationTree;
  locationsLoading: boolean;
  locationsError: Error | null;
  refreshLocations: typeof refreshLocations;
}

export interface NodeCanvasContainerOwnProps {
  tiers: LocalityTier[];
}

const NodeCanvasContainer: React.FC<
  NodeCanvasContainerReduxProps & NodeCanvasContainerOwnProps
> = props => {
  const history = useHistory();
  const {
    nodeStatuses,
    livenessStatusByNodeID,
    livenessByNodeID,
    isLoading: nodesLivenessLoading,
    nodesError,
    livenessError,
  } = useNodesSummary();

  const { refreshLocations: refreshLocationsAction } = props;
  useEffect(() => {
    refreshLocationsAction();
  }, [refreshLocationsAction]);

  // Filter out decommissioned nodes and build the locality tree, matching the
  // behavior of the former selectLocalityTree / selectCommissionedNodeStatuses
  // selectors.
  const localityTree = useMemo(
    () =>
      buildLocalityTree(
        nodeStatuses?.filter(ns => {
          const status = livenessStatusByNodeID[`${ns.desc.node_id}`];
          return (
            isNil(status) ||
            status !== LivenessStatus.NODE_STATUS_DECOMMISSIONED
          );
        }),
      ),
    [nodeStatuses, livenessStatusByNodeID],
  );

  const dataExists = !nodesLivenessLoading && !props.locationsLoading;
  const dataErrors = [nodesError, livenessError, props.locationsError].filter(
    Boolean,
  );

  const currentLocality = getLocality(localityTree, props.tiers);
  if (dataExists && isNil(currentLocality)) {
    history.replace(CLUSTERVIZ_ROOT);
  }

  return (
    <Loading
      loading={!dataExists}
      page={"node canvas container"}
      error={dataErrors}
      render={() => (
        <NodeCanvas
          localityTree={currentLocality}
          locationTree={props.locationTree}
          tiers={props.tiers}
          livenessStatuses={livenessStatusByNodeID}
          livenesses={livenessByNodeID}
        />
      )}
    />
  );
};

export default connect(
  (state: AdminUIState) => ({
    locationTree: selectLocationTree(state),
    locationsLoading: !selectLocationsRequestStatus(state).data,
    locationsError: selectLocationsRequestStatus(state).lastError,
  }),
  {
    refreshLocations,
  },
)(NodeCanvasContainer);
