// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { TimeScale } from "@cockroachlabs/cluster-ui";
import isString from "lodash/isString";
import map from "lodash/map";
import React from "react";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import { PayloadAction } from "src/interfaces/action";
import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import {
  hoverOff as hoverOffAction,
  hoverOn as hoverOnAction,
  hoverStateSelector,
  HoverState,
} from "src/redux/hover";
import {
  nodeDisplayNameByIDSelector,
  nodeIDsStringifiedSelector,
  selectStoreIDsByNodeID,
  nodeIDsSelector,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { TimeWindow, setMetricsFixedWindow } from "src/redux/timeScale";
import { nodeIDAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
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

interface NodeGraphsOwnProps {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  hoverOn: typeof hoverOnAction;
  hoverOff: typeof hoverOffAction;
  nodesQueryValid: boolean;
  livenessQueryValid: boolean;
  nodeDropdownOptions: ReturnType<
    typeof nodeDropdownOptionsSelector.resultFunc
  >;
  storeIDsByNodeID: ReturnType<typeof selectStoreIDsByNodeID.resultFunc>;
  hoverState: HoverState;
  setMetricsFixedWindow: (tw: TimeWindow) => PayloadAction<TimeWindow>;
  setTimeScale: (ts: TimeScale) => PayloadAction<TimeScale>;

  nodeIds: string[];
  nodeDisplayNameByID: ReturnType<
    typeof nodeDisplayNameByIDSelector.resultFunc
  >;
}

type RaftMessagesProps = NodeGraphsOwnProps & RouteComponentProps;

export class RaftMessages extends React.Component<RaftMessagesProps> {
  /**
   * Selector to compute node dropdown options from the current node summary
   * collection.
   */

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
    if (!isString(nodeID) || nodeID === "") {
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
    const {
      match,
      hoverState,
      hoverOn,
      hoverOff,
      storeIDsByNodeID,
      nodeIds,
      nodeDisplayNameByID,
    } = this.props;

    const selectedNode = getMatchParamByName(match, nodeIDAttr) || "";
    const nodeSources = selectedNode !== "" ? [selectedNode] : null;

    // When "all" is the selected source, some graphs display a line for every
    // node in the cluster using the nodeIDs collection. However, if a specific
    // node is already selected, these per-node graphs should only display data
    // only for the selected node.
    const nodeIDs = nodeSources ? nodeSources : nodeIds;

    // If a single node is selected, we need to restrict the set of stores
    // queried for per-store metrics (only stores that belong to that node will
    // be queried).
    const storeSources = nodeSources
      ? storeIDsForNode(storeIDsByNodeID, nodeSources[0])
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
      nodeSources,
      storeSources,
      tooltipSelection,
      nodeDisplayNameByID,
      storeIDsByNodeID,
    };

    // Generate graphs for the current dashboard, wrapping each one in a
    // MetricsDataProvider with a unique key.
    const graphs = messagesDashboard(dashboardProps);
    const graphComponents = map(graphs, (graph, idx) => {
      const key = `nodes.raftMessages.${idx}`;
      return (
        <div key={key}>
          <MetricsDataProvider
            id={key}
            key={key}
            setMetricsFixedWindow={this.props.setMetricsFixedWindow}
            setTimeScale={this.props.setTimeScale}
            history={this.props.history}
          >
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
              options={this.props.nodeDropdownOptions}
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

const nodeDropdownOptionsSelector = createSelector(
  nodeIDsSelector,
  nodeDisplayNameByIDSelector,
  (nodeIds, nodeDisplayNameByID): DropdownOption[] => {
    const base = [{ value: "", label: "Cluster" }];
    return base.concat(
      map(nodeIds, id => {
        return {
          value: id.toString(),
          label: nodeDisplayNameByID[id],
        };
      }),
    );
  },
);

const mapStateToProps = (state: AdminUIState) => ({
  nodesQueryValid: state.cachedData.nodes.valid,
  livenessQueryValid: state.cachedData.nodes.valid,
  hoverState: hoverStateSelector(state),
  nodeIds: nodeIDsStringifiedSelector(state),
  storeIDsByNodeID: selectStoreIDsByNodeID(state),
  nodeDropdownOptions: nodeDropdownOptionsSelector(state),
  nodeDisplayNameByID: nodeDisplayNameByIDSelector(state),
});

const mapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
  hoverOn: hoverOnAction,
  hoverOff: hoverOffAction,
  setMetricsFixedWindow: setMetricsFixedWindow,
  setTimeScale: setGlobalTimeScaleAction,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(RaftMessages),
);
