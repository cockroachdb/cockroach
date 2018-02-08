// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";
import PropTypes from "prop-types";
import React from "react";
import { connect } from "react-redux";
import { InjectedRouter, RouterState } from "react-router";
import { createSelector } from "reselect";

import { refreshNodes, refreshLiveness, refreshLocations } from "src/redux/apiReducers";
import { selectLocalityTree, LocalityTier, LocalityTree } from "src/redux/localities";
import { selectLocationsRequestStatus, selectLocationTree, LocationTree } from "src/redux/locations";
import {
  nodesSummarySelector,
  NodesSummary,
  selectNodeRequestStatus,
  selectLivenessRequestStatus,
  livenessStatusByNodeIDSelector,
  LivenessStatus,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { getLocality } from "src/util/localities";
import Loading from "src/views/shared/components/loading";

import { NodeCanvas } from "./nodeCanvas";
import { NodeHistory } from "./nodeHistory";

import spinner from "assets/spinner.gif";

interface HistoryAccumulatorProps {
  nodesSummary: NodesSummary;
  localityTree: LocalityTree;
  locationTree: LocationTree;
  liveness: { [id: string]: LivenessStatus };
  dataIsValid: boolean;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  refreshLocations: typeof refreshLocations;
}

interface HistoryAccumulatorOwnProps {
  tiers: LocalityTier[];
}

class HistoryAccumulator extends React.Component<HistoryAccumulatorProps & HistoryAccumulatorOwnProps, any> {
  // TODO(couchand): use withRouter instead.
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState };

  nodeHistories: { [id: string]: NodeHistory } = {};

  // accumulateHistory parses incoming nodeStatus properties and accumulates
  // a history for each node.
  accumulateHistory(props = this.props) {
    if (!props.dataIsValid) {
      return;
    }

    props.nodesSummary.nodeStatuses.map((status) => {
      const id = status.desc.node_id;
      if (!this.nodeHistories.hasOwnProperty(id)) {
        this.nodeHistories[id] = new NodeHistory(status);
      } else {
        this.nodeHistories[id].update(status);
      }
    });
  }

  componentWillMount() {
    this.accumulateHistory();
    this.props.refreshNodes();
    this.props.refreshLiveness();
    this.props.refreshLocations();
  }

  componentWillReceiveProps(props: HistoryAccumulatorProps & HistoryAccumulatorOwnProps) {
    this.accumulateHistory(props);
    props.refreshNodes();
    props.refreshLiveness();
    props.refreshLocations();
  }

  render() {
    const currentLocality = getLocality(this.props.localityTree, this.props.tiers);
    if (this.props.dataIsValid && _.isNil(currentLocality)) {
      this.context.router.replace(CLUSTERVIZ_ROOT);
    }

    return (
      <Loading
        loading={!this.props.dataIsValid}
        className="loading-image loading-image__spinner-left"
        image={spinner}
      >
        <NodeCanvas
          nodeHistories={this.nodeHistories}
          localityTree={currentLocality}
          locationTree={this.props.locationTree}
          liveness={this.props.liveness}
          tiers={this.props.tiers}
        />
      </Loading>
    );
  }
}

const selectDataIsValid = createSelector(
  selectNodeRequestStatus,
  selectLocationsRequestStatus,
  selectLivenessRequestStatus,
  (nodes, locations, liveness) => nodes.valid && locations.valid && liveness.valid,
);

export default connect(
  (state: AdminUIState, _ownProps: HistoryAccumulatorOwnProps) => ({
    nodesSummary: nodesSummarySelector(state),
    localityTree: selectLocalityTree(state),
    locationTree: selectLocationTree(state),
    liveness: livenessStatusByNodeIDSelector(state),
    dataIsValid: selectDataIsValid(state),
  }),
  {
    refreshNodes,
    refreshLiveness,
    refreshLocations,
  },
)(HistoryAccumulator);
