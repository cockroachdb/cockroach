import _ from "lodash";
import React from "react";
import PropTypes from "prop-types";
import { Helmet } from "react-helmet";
import { InjectedRouter, RouterState } from "react-router";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import {
  nodeIDAttr, dashboardNameAttr,
} from "src/util/constants";

import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import TimeScaleDropdown from "src/views/cluster/containers/timescale";
import AggregationSelector from "src/views/shared/containers/aggregationSelector";
import ClusterSummaryBar from "./summaryBar";

import { AdminUIState } from "src/redux/state";
import { AggregationLevel, selectAggregationLevel } from "src/redux/aggregationLevel";
import { refreshNodes, refreshLiveness } from "src/redux/apiReducers";
import { hoverStateSelector, HoverState, hoverOn, hoverOff } from "src/redux/hover";
import { nodesSummarySelector, NodesSummary, LivenessStatus } from "src/redux/nodes";
import Alerts from "src/views/shared/containers/alerts";

import { DashboardsPage } from "src/views/metrics";

interface GraphDashboard {
  label: string;
}

const dashboards: {[key: string]: GraphDashboard} = {
  "overview" : { label: "Overview" },
  "hardware": { label: "Hardware" },
  "runtime" : { label: "Runtime" },
  "sql": { label: "SQL" },
  "storage": { label: "Storage" },
  "replication": { label: "Replication" },
  "distributed": { label: "Distributed" },
  "queues": { label: "Queues" },
  "requests": { label: "Slow Requests" },
};

const defaultDashboard = "overview";

const dashboardDropdownOptions = _.map(dashboards, (dashboard, key) => {
  return {
    value: key,
    label: dashboard.label,
  };
});

// The properties required by a NodeGraphs component.
interface NodeGraphsOwnProps {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  hoverOn: typeof hoverOn;
  hoverOff: typeof hoverOff;
  nodesQueryValid: boolean;
  livenessQueryValid: boolean;
  nodesSummary: NodesSummary;
  hoverState: HoverState;
  aggregationLevel: AggregationLevel;
}

type NodeGraphsProps = NodeGraphsOwnProps & RouterState;

/**
 * NodeGraphs renders the main content of the cluster graphs page.
 */
class NodeGraphs extends React.Component<NodeGraphsProps, {}> {
  // Magic to add react router to the context.
  // See https://github.com/ReactTraining/react-router/issues/975
  // TODO(mrtracy): Switch this, and the other uses of contextTypes, to use the
  // 'withRouter' HoC after upgrading to react-router 4.x.
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState; };

  /**
   * Selector to compute node dropdown options from the current node summary
   * collection.
   */
  private nodeDropdownOptions = createSelector(
    (summary: NodesSummary) => summary.nodeStatuses,
    (summary: NodesSummary) => summary.nodeDisplayNameByID,
    (summary: NodesSummary) => summary.livenessStatusByNodeID,
    (nodeStatuses, nodeDisplayNameByID, livenessStatusByNodeID): DropdownOption[] => {
      const base = [{value: "", label: "Cluster"}];
      return base.concat(
        _.chain(nodeStatuses)
          .filter(ns => livenessStatusByNodeID[ns.desc.node_id] !== LivenessStatus.DECOMMISSIONED)
          .map(ns => ({
            value: ns.desc.node_id.toString(),
            label: nodeDisplayNameByID[ns.desc.node_id],
          }))
          .value(),
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

  setClusterPath(nodeID: string, dashboardName: string, aggregationLevel?: AggregationLevel) {
    if (!_.isString(nodeID) || nodeID === "") {
      const query = aggregationLevel && aggregationLevel !== AggregationLevel.Cluster ? `?agg=${aggregationLevel}` : "";
      this.context.router.push(`/metrics/${dashboardName}/cluster${query}`);
    } else {
      this.context.router.push(`/metrics/${dashboardName}/node/${nodeID}`);
    }
  }

  nodeChange = (selected: DropdownOption) => {
    this.setClusterPath(selected.value, this.props.params[dashboardNameAttr]);
  }

  dashChange = (selected: DropdownOption) => {
    this.setClusterPath(this.props.params[nodeIDAttr], selected.value, this.props.aggregationLevel);
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(props: NodeGraphsProps) {
    this.refresh(props);
  }

  render() {
    const { params, nodesSummary, aggregationLevel } = this.props;
    const selectedDashboard = params[dashboardNameAttr];
    const dashboard = _.has(dashboards, selectedDashboard)
      ? selectedDashboard
      : defaultDashboard;

    const title = dashboards[dashboard].label + " Dashboard";
    const selectedNode = params[nodeIDAttr] || "";
    const nodeSources = (selectedNode !== "") ? [selectedNode] : null;

    // When "all" is the selected source, some graphs display a line for every
    // node in the cluster using the nodeIDs collection. However, if a specific
    // node is already selected, these per-node graphs should only display data
    // only for the selected node.
    const nodeIDs = nodeSources ? nodeSources : nodesSummary.nodeIDs;

    // tooltipSelection is a string used in tooltips to reference the currently
    // selected nodes. This is a prepositional phrase, currently either "across
    // all nodes" or "on node X".
    const tooltipSelection = (nodeSources && nodeSources.length === 1)
                              ? `on node ${nodeSources[0]}`
                              : "across all nodes";

    const forwardProps = {
      hoverOn: this.props.hoverOn,
      hoverOff: this.props.hoverOff,
      hoverState: this.props.hoverState,
    };

    return (
      <div>
        <Helmet>
          <title>{ title }</title>
        </Helmet>
        <section className="section"><h1>{ title }</h1></section>
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
            />
          </PageConfigItem>
          {
            nodeSources ? null : (
              <PageConfigItem>
                <AggregationSelector />
              </PageConfigItem>
            )
          }
          <PageConfigItem>
            <TimeScaleDropdown />
          </PageConfigItem>
        </PageConfig>
        <section className="section">
          <div className="l-columns">
            <div className="chart-group l-columns__left">
              <DashboardsPage
                dashboard="overview"
                aggregationLevel={aggregationLevel}
                nodeSources={nodeIDs}
                nodesSummary={nodesSummary}
                tooltipSelection={tooltipSelection}
                forwardProps={forwardProps}
              />
            </div>
            <div className="l-columns__right">
              <Alerts />
              <ClusterSummaryBar nodesSummary={this.props.nodesSummary} nodeSources={nodeSources} />
            </div>
          </div>
        </section>
      </div>
    );
  }
}

export default connect(
  (state: AdminUIState) => {
    return {
      nodesSummary: nodesSummarySelector(state),
      nodesQueryValid: state.cachedData.nodes.valid,
      livenessQueryValid: state.cachedData.nodes.valid,
      hoverState: hoverStateSelector(state),
      aggregationLevel: selectAggregationLevel(state),
    };
  },
  {
    refreshNodes,
    refreshLiveness,
    hoverOn,
    hoverOff,
  },
)(NodeGraphs);
