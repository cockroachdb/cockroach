// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import { withRouter, RouteComponentProps } from "react-router-dom";
import { createSelector } from "reselect";

import { cockroach } from "src/js/protos";
import {
  refreshNodes,
  refreshLiveness,
  refreshLocations,
} from "src/redux/apiReducers";
import {
  selectLocalityTree,
  LocalityTier,
  LocalityTree,
} from "src/redux/localities";
import {
  selectLocationsRequestStatus,
  selectLocationTree,
  LocationTree,
} from "src/redux/locations";
import {
  nodesSummarySelector,
  NodesSummary,
  selectNodeRequestStatus,
  selectLivenessRequestStatus,
  livenessStatusByNodeIDSelector,
  LivenessStatus,
  livenessByNodeIDSelector,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { getLocality } from "src/util/localities";
import { Loading } from "@cockroachlabs/cluster-ui";
import { NodeCanvas } from "./nodeCanvas";

type Liveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;

interface NodeCanvasContainerProps {
  nodesSummary: NodesSummary;
  localityTree: LocalityTree;
  locationTree: LocationTree;
  livenessStatuses: { [id: string]: LivenessStatus };
  livenesses: { [id: string]: Liveness };
  dataExists: boolean;
  dataIsValid: boolean;
  dataErrors: Error[];
  refreshNodes: typeof refreshNodes;
  refreshLiveness: any;
  refreshLocations: any;
}

export interface NodeCanvasContainerOwnProps {
  tiers: LocalityTier[];
}

class NodeCanvasContainer extends React.Component<
  NodeCanvasContainerProps & NodeCanvasContainerOwnProps & RouteComponentProps
> {
  componentDidMount() {
    this.props.refreshNodes();
    this.props.refreshLiveness();
    this.props.refreshLocations();
  }

  componentDidUpdate() {
    this.props.refreshNodes();
    this.props.refreshLiveness();
    this.props.refreshLocations();
  }

  render() {
    const currentLocality = getLocality(
      this.props.localityTree,
      this.props.tiers,
    );
    if (this.props.dataIsValid && _.isNil(currentLocality)) {
      this.props.history.replace(CLUSTERVIZ_ROOT);
    }

    return (
      <Loading
        loading={!this.props.dataExists}
        error={this.props.dataErrors}
        render={() => (
          <NodeCanvas
            localityTree={currentLocality}
            locationTree={this.props.locationTree}
            tiers={this.props.tiers}
            livenessStatuses={this.props.livenessStatuses}
            livenesses={this.props.livenesses}
          />
        )}
      />
    );
  }
}

const selectDataExists = createSelector(
  selectNodeRequestStatus,
  selectLocationsRequestStatus,
  selectLivenessRequestStatus,
  (nodes, locations, liveness) =>
    !!nodes.data && !!locations.data && !!liveness.data,
);

const selectDataIsValid = createSelector(
  selectNodeRequestStatus,
  selectLocationsRequestStatus,
  selectLivenessRequestStatus,
  (nodes, locations, liveness) =>
    nodes.valid && locations.valid && liveness.valid,
);

const dataErrors = createSelector(
  selectNodeRequestStatus,
  selectLocationsRequestStatus,
  selectLivenessRequestStatus,
  (nodes, locations, liveness) => [
    nodes.lastError,
    locations.lastError,
    liveness.lastError,
  ],
);

export default withRouter(
  connect(
    (state: AdminUIState, _ownProps: NodeCanvasContainerOwnProps) => ({
      nodesSummary: nodesSummarySelector(state),
      localityTree: selectLocalityTree(state),
      locationTree: selectLocationTree(state),
      livenessStatuses: livenessStatusByNodeIDSelector(state),
      livenesses: livenessByNodeIDSelector(state),
      dataIsValid: selectDataIsValid(state),
      dataExists: selectDataExists(state),
      dataErrors: dataErrors(state),
    }),
    {
      refreshNodes,
      refreshLiveness,
      refreshLocations,
    },
  )(NodeCanvasContainer),
);
