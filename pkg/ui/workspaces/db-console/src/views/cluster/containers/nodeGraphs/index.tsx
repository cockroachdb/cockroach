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
import ClusterSummaryBar from "./summaryBar";

import { AdminUIState } from "src/redux/state";
import {
  refreshNodes,
  refreshLiveness,
  refreshSettings,
} from "src/redux/apiReducers";
import {
  hoverStateSelector,
  HoverState,
  hoverOn,
  hoverOff,
} from "src/redux/hover";
import {
  LivenessStatus,
  nodeDisplayNameByIDSelector,
  livenessStatusByNodeIDSelector,
  nodeIDsSelector,
  nodeIDsStringifiedSelector,
  selectStoreIDsByNodeID,
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
import ttlDashboard from "./dashboards/ttl";
import crossClusterReplicationDashboard from "./dashboards/crossClusterReplication";
import { getMatchParamByName } from "src/util/query";
import { PayloadAction } from "src/interfaces/action";
import {
  setMetricsFixedWindow,
  TimeWindow,
  adjustTimeScale,
  setTimeScale,
  selectTimeScale,
} from "src/redux/timeScale";
import { InlineAlert } from "src/components";
import {
  Anchor,
  TimeScaleDropdown,
  TimeScale,
} from "@cockroachlabs/cluster-ui";
import { reduceStorageOfTimeSeriesDataOperationalFlags } from "src/util/docs";
import moment from "moment-timezone";
import {
  selectResolution10sStorageTTL,
  selectResolution30mStorageTTL,
  selectCrossClusterReplicationEnabled,
} from "src/redux/clusterSettings";
import { getDataFromServer } from "src/util/dataFromServer";

interface GraphDashboard {
  label: string;
  component: (props: GraphDashboardProps) => React.ReactElement<any>[];
  isKvDashboard: boolean;
}

const dashboards: { [key: string]: GraphDashboard } = {
  overview: {
    label: "Overview",
    component: overviewDashboard,
    isKvDashboard: false,
  },
  hardware: {
    label: "Hardware",
    component: hardwareDashboard,
    isKvDashboard: true,
  },
  runtime: {
    label: "Runtime",
    component: runtimeDashboard,
    isKvDashboard: true,
  },
  sql: { label: "SQL", component: sqlDashboard, isKvDashboard: false },
  storage: {
    label: "Storage",
    component: storageDashboard,
    isKvDashboard: false,
  },
  replication: {
    label: "Replication",
    component: replicationDashboard,
    isKvDashboard: true,
  },
  distributed: {
    label: "Distributed",
    component: distributedDashboard,
    isKvDashboard: true,
  },
  queues: { label: "Queues", component: queuesDashboard, isKvDashboard: true },
  requests: {
    label: "Slow Requests",
    component: requestsDashboard,
    isKvDashboard: true,
  },
  changefeeds: {
    label: "Changefeeds",
    component: changefeedsDashboard,
    isKvDashboard: false,
  },
  overload: {
    label: "Overload",
    component: overloadDashboard,
    isKvDashboard: true,
  },
  ttl: { label: "TTL", component: ttlDashboard, isKvDashboard: false },
  crossClusterReplication: {
    label: "Cross-Cluster Replication",
    component: crossClusterReplicationDashboard,
    isKvDashboard: true,
  },
};

const defaultDashboard = "overview";

const dashboardDropdownOptions = _.map(dashboards, (dashboard, key) => {
  return {
    value: key,
    label: dashboard.label,
    isKvDashboard: dashboard.isKvDashboard,
  };
});

type MapStateToProps = {
  hoverState: HoverState;
  resolution10sStorageTTL: moment.Duration;
  resolution30mStorageTTL: moment.Duration;
  timeScale: TimeScale;
  nodeDropdownOptions: ReturnType<
    typeof nodeDropdownOptionsSelector.resultFunc
  >;
  nodeIds: string[];
  storeIDsByNodeID: ReturnType<typeof selectStoreIDsByNodeID.resultFunc>;
  nodeDisplayNameByID: ReturnType<
    typeof nodeDisplayNameByIDSelector.resultFunc
  >;
  crossClusterReplicationEnabled: boolean;
};

type MapDispatchToProps = {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  refreshNodeSettings: typeof refreshSettings;
  hoverOn: typeof hoverOn;
  hoverOff: typeof hoverOff;
  setMetricsFixedWindow: (tw: TimeWindow) => PayloadAction<TimeWindow>;
  setTimeScale: (ts: TimeScale) => PayloadAction<TimeScale>;
};

type NodeGraphsProps = RouteComponentProps &
  MapStateToProps &
  MapDispatchToProps;

type NodeGraphsState = {
  showLowResolutionAlert: boolean;
  showDeletedDataAlert: boolean;
};

/**
 * NodeGraphs renders the main content of the cluster graphs page.
 */
export class NodeGraphs extends React.Component<
  NodeGraphsProps,
  NodeGraphsState
> {
  constructor(props: NodeGraphsProps) {
    super(props);
    this.state = {
      showDeletedDataAlert: false,
      showLowResolutionAlert: false,
    };
  }

  refresh = () => {
    this.props.refreshNodes();
    this.props.refreshLiveness();
    this.props.refreshNodeSettings();
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
    // settings won't change frequently so it's safe to request one
    // when page is loaded.
    this.props.refreshNodeSettings();
  }

  componentDidUpdate() {
    this.refresh();
  }

  adjustTimeScaleOnChange = (
    curTimeScale: TimeScale,
    timeWindow: TimeWindow,
  ): TimeScale => {
    const { resolution10sStorageTTL, resolution30mStorageTTL } = this.props;
    const adjustedTimeScale = adjustTimeScale(
      curTimeScale,
      timeWindow,
      resolution10sStorageTTL,
      resolution30mStorageTTL,
    );
    switch (adjustedTimeScale.adjustmentReason) {
      case "low_resolution_period":
        this.setState({
          showLowResolutionAlert: true,
          showDeletedDataAlert: false,
        });
        break;
      case "deleted_data_period":
        this.setState({
          showLowResolutionAlert: false,
          showDeletedDataAlert: true,
        });
        break;
      default:
        this.setState({
          showLowResolutionAlert: false,
          showDeletedDataAlert: false,
        });
        break;
    }
    return adjustedTimeScale.timeScale;
  };

  render() {
    const {
      match,
      nodeDropdownOptions,
      storeIDsByNodeID,
      nodeDisplayNameByID,
      nodeIds,
    } = this.props;
    const canViewKvGraphs =
      getDataFromServer().FeatureFlags.can_view_kv_metric_dashboards;
    const { showLowResolutionAlert, showDeletedDataAlert } = this.state;
    let selectedDashboard = getMatchParamByName(match, dashboardNameAttr);
    if (dashboards[selectedDashboard].isKvDashboard && !canViewKvGraphs) {
      selectedDashboard = defaultDashboard;
    }
    const dashboard = _.has(dashboards, selectedDashboard)
      ? selectedDashboard
      : defaultDashboard;

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

    const forwardParams = {
      hoverOn: this.props.hoverOn,
      hoverOff: this.props.hoverOff,
      hoverState: this.props.hoverState,
    };

    // Generate graphs for the current dashboard, wrapping each one in a
    // MetricsDataProvider with a unique key.
    const graphs = dashboards[dashboard]
      .component(dashboardProps)
      .filter(d => canViewKvGraphs || !d.props.isKvGraph);
    const graphComponents = _.map(graphs, (graph, idx) => {
      const key = `nodes.${dashboard}.${idx}`;
      return (
        <MetricsDataProvider
          id={key}
          key={key}
          setMetricsFixedWindow={this.props.setMetricsFixedWindow}
          setTimeScale={this.props.setTimeScale}
          history={this.props.history}
          adjustTimeScaleOnChange={this.adjustTimeScaleOnChange}
        >
          {React.cloneElement(graph, forwardParams)}
        </MetricsDataProvider>
      );
    });

    // add padding to have last chart tooltip visible
    // tooltip layout with header and paddings take up
    // somewhere around 50px, after it have more than
    // 9 nodes it switch to multicolumn layout that take
    // somewhere around 90px height + 10px for per node
    // as we have 3 columns, we divide node amount on 3
    const paddingBottom =
      nodeIDs.length > 8 ? 90 + Math.ceil(nodeIDs.length / 3) * 10 : 50;
    const filteredDropdownOptions = dashboardDropdownOptions
      // Don't show KV dashboards if the logged-in user doesn't have permission to view them.
      .filter(option => canViewKvGraphs || !option.isKvDashboard)
      // Don't show the replication dashboard if not enabled.
      .filter(
        option =>
          this.props.crossClusterReplicationEnabled ||
          option.label !== "Cross-Cluster Replication",
      );

    return (
      <div style={{ paddingBottom }}>
        <Helmet title={"Metrics"} />
        <h3 className="base-heading">Metrics</h3>
        <PageConfig>
          <PageConfigItem>
            <Dropdown
              title="Graph"
              options={nodeDropdownOptions}
              selected={selectedNode}
              onChange={this.nodeChange}
            />
          </PageConfigItem>
          <PageConfigItem>
            <Dropdown
              title="Dashboard"
              options={filteredDropdownOptions}
              selected={dashboard}
              onChange={this.dashChange}
              className="full-size"
            />
          </PageConfigItem>
          <PageConfigItem>
            <TimeScaleDropdown
              currentScale={this.props.timeScale}
              setTimeScale={this.props.setTimeScale}
              adjustTimeScaleOnChange={this.adjustTimeScaleOnChange}
            />
          </PageConfigItem>
        </PageConfig>
        <section className="section">
          {showLowResolutionAlert && (
            <InlineAlert
              title="Some data in this timeframe is shown at lower resolution."
              intent="warning"
              message={
                <span>
                  The 'timeseries.storage.resolution_10s.ttl' cluster setting
                  determines how long data is stored at 10-second resolution.
                  The remaining data is stored at 30-minute resolution. To
                  configure these settings, refer to{" "}
                  <Anchor
                    href={reduceStorageOfTimeSeriesDataOperationalFlags}
                    target="_blank"
                  >
                    the docs
                  </Anchor>
                  .
                </span>
              }
            />
          )}
          {showDeletedDataAlert && (
            <InlineAlert
              title="Some data in this timeframe is no longer stored."
              intent="warning"
              message={
                <span>
                  The 'timeseries.storage.resolution_30m.ttl' cluster setting
                  determines how long data is stored at 30-minute resolution.
                  Data is no longer stored after this time period. To configure
                  this setting, refer to{" "}
                  <Anchor
                    href={reduceStorageOfTimeSeriesDataOperationalFlags}
                    target="_blank"
                  >
                    the docs
                  </Anchor>
                  .
                </span>
              }
            />
          )}
        </section>
        <section className="section">
          <div className="l-columns">
            <div className="chart-group l-columns__left">{graphComponents}</div>
            <div className="l-columns__right">
              <Alerts />
              <ClusterSummaryBar nodeSources={nodeSources} />
            </div>
          </div>
        </section>
      </div>
    );
  }
}

/**
 * Selector to compute node dropdown options from the current node summary
 * collection.
 */
const nodeDropdownOptionsSelector = createSelector(
  nodeIDsSelector,
  nodeDisplayNameByIDSelector,
  livenessStatusByNodeIDSelector,
  (nodeIds, nodeDisplayNameByID, livenessStatusByNodeID): DropdownOption[] => {
    const base = [{ value: "", label: "Cluster" }];
    return base.concat(
      _.chain(nodeIds)
        .filter(
          id =>
            livenessStatusByNodeID[id] !==
            LivenessStatus.NODE_STATUS_DECOMMISSIONED,
        )
        .map(id => ({
          value: id.toString(),
          label: nodeDisplayNameByID[id],
        }))
        .value(),
    );
  },
);

const mapStateToProps = (state: AdminUIState): MapStateToProps => ({
  hoverState: hoverStateSelector(state),
  resolution10sStorageTTL: selectResolution10sStorageTTL(state),
  resolution30mStorageTTL: selectResolution30mStorageTTL(state),
  timeScale: selectTimeScale(state),
  nodeIds: nodeIDsStringifiedSelector(state),
  storeIDsByNodeID: selectStoreIDsByNodeID(state),
  nodeDropdownOptions: nodeDropdownOptionsSelector(state),
  nodeDisplayNameByID: nodeDisplayNameByIDSelector(state),
  crossClusterReplicationEnabled: selectCrossClusterReplicationEnabled(state),
});

const mapDispatchToProps: MapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
  refreshNodeSettings: refreshSettings,
  hoverOn,
  hoverOff,
  setMetricsFixedWindow: setMetricsFixedWindow,
  setTimeScale: setTimeScale,
};

export default compose(
  withRouter,
  connect(mapStateToProps, mapDispatchToProps),
)(NodeGraphs);
