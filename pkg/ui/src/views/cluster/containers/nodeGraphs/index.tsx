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
import { Helmet } from "react-helmet";
import { compose } from "redux";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import { withRouter, RouteComponentProps } from "react-router-dom";

import { nodeIDAttr, dashboardNameAttr } from "src/util/constants";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";
import TimeScaleDropdown from "src/views/cluster/containers/timescale";
import ClusterSummaryBar from "./summaryBar";

import { AdminUIState } from "src/redux/state";
import { refreshNodes, refreshLiveness } from "src/redux/apiReducers";
import {
  hoverStateSelector,
  HoverState,
  hoverOn,
  hoverOff,
} from "src/redux/hover";
import {
  nodesSummarySelector,
  NodesSummary,
  LivenessStatus,
  selectNodesSummaryValid,
} from "src/redux/nodes";
import Alerts from "src/views/shared/containers/alerts";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";

import {
  GraphDashboardProps,
  storeIDsForNode,
} from "./dashboards/dashboardUtils";

import overviewDashboard from "./dashboards/overview";
import runtimeDashboard from "./dashboards/runtime";
import sqlDashboard from "./dashboards/sql";
import storageDashboard from "./dashboards/storage";
import replicationDashboard from "./dashboards/replication";
import distributedDashboard from "./dashboards/distributed";
import queuesDashboard from "./dashboards/queues";
import requestsDashboard from "./dashboards/requests";
import hardwareDashboard from "./dashboards/hardware";
import changefeedsDashboard from "./dashboards/changefeeds";
import overloadDashboard from "./dashboards/overload";
import { getMatchParamByName } from "src/util/query";
import { PayloadAction } from "src/interfaces/action";
import {
  setTimeRange,
  setTimeScale,
  TimeWindow,
  TimeScale,
} from "src/redux/timewindow";
interface GraphDashboard {
  label: string;
  component: (props: GraphDashboardProps) => React.ReactElement<any>[];
}

const dashboards: { [key: string]: GraphDashboard } = {
  overview: { label: "Overview", component: overviewDashboard },
  hardware: { label: "Hardware", component: hardwareDashboard },
  runtime: { label: "Runtime", component: runtimeDashboard },
  sql: { label: "SQL", component: sqlDashboard },
  storage: { label: "Storage", component: storageDashboard },
  replication: { label: "Replication", component: replicationDashboard },
  distributed: { label: "Distributed", component: distributedDashboard },
  queues: { label: "Queues", component: queuesDashboard },
  requests: { label: "Slow Requests", component: requestsDashboard },
  changefeeds: { label: "Changefeeds", component: changefeedsDashboard },
  overload: { label: "Overload", component: overloadDashboard },
};

const defaultDashboard = "overview";

const dashboardDropdownOptions = _.map(dashboards, (dashboard, key) => {
  return {
    value: key,
    label: dashboard.label,
  };
});

type MapStateToProps = {
  nodesSummary: NodesSummary;
  hoverState: HoverState;
  nodesSummaryValid: boolean;
};

type MapDispatchToProps = {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  hoverOn: typeof hoverOn;
  hoverOff: typeof hoverOff;
  setTimeRange: (tw: TimeWindow) => PayloadAction<TimeWindow>;
  setTimeScale: (ts: TimeScale) => PayloadAction<TimeScale>;
};

type NodeGraphsProps = RouteComponentProps &
  MapStateToProps &
  MapDispatchToProps;

/**
 * NodeGraphs renders the main content of the cluster graphs page.
 */
export class NodeGraphs extends React.Component<NodeGraphsProps> {
  /**
   * Selector to compute node dropdown options from the current node summary
   * collection.
   */
  private nodeDropdownOptions = createSelector(
    (summary: NodesSummary) => summary.nodeStatuses,
    (summary: NodesSummary) => summary.nodeDisplayNameByID,
    (summary: NodesSummary) => summary.livenessStatusByNodeID,
    (
      nodeStatuses,
      nodeDisplayNameByID,
      livenessStatusByNodeID,
    ): DropdownOption[] => {
      const base = [{ value: "", label: "Cluster" }];
      return base.concat(
        _.chain(nodeStatuses)
          .filter(
            (ns) =>
              livenessStatusByNodeID[ns.desc.node_id] !==
              LivenessStatus.NODE_STATUS_DECOMMISSIONED,
          )
          .map((ns) => ({
            value: ns.desc.node_id.toString(),
            label: nodeDisplayNameByID[ns.desc.node_id],
          }))
          .value(),
      );
    },
  );

  refresh = () => {
    this.props.refreshNodes();
    this.props.refreshLiveness();
  };

  setClusterPath(nodeID: string, dashboardName: string) {
    const push = this.props.history.push;
    if (!_.isString(nodeID) || nodeID === "") {
      push(`/metrics/${dashboardName}/cluster`);
    } else {
      push(`/metrics/${dashboardName}/node/${nodeID}`);
    }
  }

  nodeChange = (selected: DropdownOption) => {
    this.setClusterPath(
      selected.value,
      getMatchParamByName(this.props.match, dashboardNameAttr),
    );
  };

  dashChange = (selected: DropdownOption) => {
    this.setClusterPath(
      getMatchParamByName(this.props.match, nodeIDAttr),
      selected.value,
    );
  };

  componentDidMount() {
    this.refresh();
  }

  componentDidUpdate() {
    this.refresh();
  }

  render() {
    const { match, nodesSummary } = this.props;
    const selectedDashboard = getMatchParamByName(match, dashboardNameAttr);
    const dashboard = _.has(dashboards, selectedDashboard)
      ? selectedDashboard
      : defaultDashboard;

    const title = dashboards[dashboard].label + " Dashboard";
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

    const forwardParams = {
      hoverOn: this.props.hoverOn,
      hoverOff: this.props.hoverOff,
      hoverState: this.props.hoverState,
    };

    // Generate graphs for the current dashboard, wrapping each one in a
    // MetricsDataProvider with a unique key.
    const graphs = dashboards[dashboard].component(dashboardProps);
    const graphComponents = _.map(graphs, (graph, idx) => {
      const key = `nodes.${dashboard}.${idx}`;
      return (
        <MetricsDataProvider
          id={key}
          key={key}
          setTimeRange={this.props.setTimeRange}
          setTimeScale={this.props.setTimeScale}
          history={this.props.history}
        >
          {React.cloneElement(graph, forwardParams)}
        </MetricsDataProvider>
      );
    });

    // add pading to have last chart tooltip visible
    // tooltip layout with header and paddings take up
    // somewhere around 50px, after it have more than
    // 9 nodes it switch to multicolumn layout that take
    // somewhere around 90px height + 10px for per node
    // as we have 3 columns, we divide node amount on 3
    const paddingBottom =
      nodeIDs.length > 8 ? 90 + Math.ceil(nodeIDs.length / 3) * 10 : 50;

    return (
      <div style={{ paddingBottom }}>
        <Helmet title={title} />
        <section className="section">
          <h1 className="base-heading">{title}</h1>
        </section>
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
            <Dropdown
              title="Dashboard"
              options={dashboardDropdownOptions}
              selected={dashboard}
              onChange={this.dashChange}
              className="full-size"
            />
          </PageConfigItem>
          <PageConfigItem>
            <TimeScaleDropdown />
          </PageConfigItem>
        </PageConfig>
        <section className="section">
          <div className="l-columns">
            <div className="chart-group l-columns__left">{graphComponents}</div>
            <div className="l-columns__right">
              <Alerts />
              <ClusterSummaryBar
                nodesSummary={this.props.nodesSummary}
                nodeSources={nodeSources}
                nodesSummaryValid={this.props.nodesSummaryValid}
              />
            </div>
          </div>
        </section>
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState): MapStateToProps => ({
  nodesSummary: nodesSummarySelector(state),
  hoverState: hoverStateSelector(state),
  nodesSummaryValid: selectNodesSummaryValid(state),
});

const mapDispatchToProps: MapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
  hoverOn,
  hoverOff,
  setTimeRange: setTimeRange,
  setTimeScale: setTimeScale,
};

export default compose(
  withRouter,
  connect(mapStateToProps, mapDispatchToProps),
)(NodeGraphs);
