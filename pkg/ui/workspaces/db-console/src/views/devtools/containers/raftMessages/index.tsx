// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import {
  hoverOff as hoverOffAction,
  hoverOn as hoverOnAction,
  hoverStateSelector,
  HoverState,
} from "src/redux/hover";
import { NodesSummary, nodesSummarySelector } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import {
  GraphDashboardProps,
  storeIDsForNode,
} from "src/views/cluster/containers/nodeGraphs/dashboards/dashboardUtils";
import TimeScaleDropdown from "src/views/cluster/containers/timeScaleDropdownWithSearchParams";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";
import messagesDashboard from "./messages";
import { getMatchParamByName } from "src/util/query";

interface NodeGraphsOwnProps {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  hoverOn: typeof hoverOnAction;
  hoverOff: typeof hoverOffAction;
  nodesQueryValid: boolean;
  livenessQueryValid: boolean;
  nodesSummary: NodesSummary;
  hoverState: HoverState;
}

type RaftMessagesProps = NodeGraphsOwnProps & RouteComponentProps;

export class RaftMessages extends React.Component<RaftMessagesProps> {
  /**
   * Selector to compute node dropdown options from the current node summary
   * collection.
   */
  private nodeDropdownOptions = createSelector(
    (summary: NodesSummary) => summary.nodeStatuses,
    (summary: NodesSummary) => summary.nodeDisplayNameByID,
    (nodeStatuses, nodeDisplayNameByID): DropdownOption[] => {
      const base = [{ value: "", label: "Cluster" }];
      return base.concat(
        _.map(nodeStatuses, ns => {
          return {
            value: ns.desc.node_id.toString(),
            label: nodeDisplayNameByID[ns.desc.node_id],
          };
        }),
      );
    },
  );

  refresh(props = this.props) {
    if (!props.nodesQueryValid) {
      props.refreshNodes();
    }
    if (!props.livenessQueryValid) {
      props.refreshLiveness();
    }
  }

  setClusterPath(nodeID: string) {
    const push = this.props.history.push;
    if (!_.isString(nodeID) || nodeID === "") {
      push("/raft/messages/all/");
    } else {
      push(`/raft/messages/node/${nodeID}`);
    }
  }

  nodeChange = (selected: DropdownOption) => {
    this.setClusterPath(selected.value);
  };

  componentDidMount() {
    this.refresh();
  }

  componentDidUpdate(props: RaftMessagesProps) {
    this.refresh(props);
  }

  render() {
    const { match, nodesSummary, hoverState, hoverOn, hoverOff } = this.props;

    const selectedNode = getMatchParamByName(match, nodeIDAttr) || "";
    const nodeSources = selectedNode !== "" ? [selectedNode] : null;

    // When "all" is the selected source, some graphs display a line for every
    // node in the cluster using the nodeIDs collection. However, if a specific
    // node is already selected, these per-node graphs should only display data
    // only for the selected node.
    const nodeIDs = nodeSources ? nodeSources : nodesSummary.nodeIDs;

    // If a single node is selected, we need to restrict the set of stores
    // queried for per-store metrics (only stores that belong to that node will
    // be queried).
    const storeSources = nodeSources
      ? storeIDsForNode(nodesSummary, nodeSources[0])
      : null;

    // tooltipSelection is a string used in tooltips to reference the currently
    // selected nodes. This is a prepositional phrase, currently either "across
    // all nodes" or "on node X".
    const tooltipSelection =
      nodeSources && nodeSources.length === 1
        ? `on node ${nodeSources[0]}`
        : "across all nodes";

    const dashboardProps: GraphDashboardProps = {
      nodeIDs,
      nodesSummary,
      nodeSources,
      storeSources,
      tooltipSelection,
    };

    // Generate graphs for the current dashboard, wrapping each one in a
    // MetricsDataProvider with a unique key.
    const graphs = messagesDashboard(dashboardProps);
    const graphComponents = _.map(graphs, (graph, idx) => {
      const key = `nodes.raftMessages.${idx}`;
      return (
        <div key={key}>
          <MetricsDataProvider id={key}>
            {React.cloneElement(graph, { hoverOn, hoverOff, hoverState })}
          </MetricsDataProvider>
        </div>
      );
    });

    return (
      <div>
        <PageConfig>
          <PageConfigItem>
            <Dropdown
              title="Graph"
              options={this.nodeDropdownOptions(this.props.nodesSummary)}
              selected={selectedNode}
              onChange={this.nodeChange}
            />
          </PageConfigItem>
          <PageConfigItem>
            <TimeScaleDropdown />
          </PageConfigItem>
        </PageConfig>
        <div className="section l-columns">
          <div className="chart-group l-columns__left">{graphComponents}</div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState) => ({
  // RootState contains declaration for whole state
  nodesSummary: nodesSummarySelector(state),
  nodesQueryValid: state.cachedData.nodes.valid,
  livenessQueryValid: state.cachedData.nodes.valid,
  hoverState: hoverStateSelector(state),
});

const mapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
  hoverOn: hoverOnAction,
  hoverOff: hoverOffAction,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(RaftMessages),
);
