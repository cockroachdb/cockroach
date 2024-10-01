// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Anchor, TimeScale } from "@cockroachlabs/cluster-ui";
import has from "lodash/has";
import map from "lodash/map";
import moment from "moment-timezone";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { withRouter, RouteComponentProps } from "react-router-dom";
import { createSelector } from "reselect";

import { InlineAlert } from "src/components";
import { PayloadAction } from "src/interfaces/action";
import {
  refreshNodes,
  refreshLiveness,
  refreshSettings,
  refreshTenantsList,
} from "src/redux/apiReducers";
import {
  selectResolution10sStorageTTL,
  selectResolution30mStorageTTL,
} from "src/redux/clusterSettings";
import { getCookieValue } from "src/redux/cookies";
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
  nodeDisplayNameByIDSelectorWithoutAddress,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import {
  containsApplicationTenants,
  isSystemTenant,
  tenantDropdownOptions,
} from "src/redux/tenants";
import {
  setMetricsFixedWindow,
  TimeWindow,
  adjustTimeScale,
  setTimeScale,
  selectTimeScale,
} from "src/redux/timeScale";
import {
  nodeIDAttr,
  dashboardNameAttr,
  tenantNameAttr,
} from "src/util/constants";
import { getDataFromServer } from "src/util/dataFromServer";
import { reduceStorageOfTimeSeriesDataOperationalFlags } from "src/util/docs";
import { getMatchParamByName } from "src/util/query";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";
import Alerts from "src/views/shared/containers/alerts";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";

import TimeScaleDropdown from "../timeScaleDropdownWithSearchParams";

import changefeedsDashboard from "./dashboards/changefeeds";
import crossClusterReplicationDashboard from "./dashboards/crossClusterReplication";
import {
  GraphDashboardProps,
  storeIDsForNode,
} from "./dashboards/dashboardUtils";
import distributedDashboard from "./dashboards/distributed";
import hardwareDashboard from "./dashboards/hardware";
import logicalDataReplicationDashboard from "./dashboards/logicalDataReplication";
import networkingDashboard from "./dashboards/networking";
import overloadDashboard from "./dashboards/overload";
import overviewDashboard from "./dashboards/overview";
import queuesDashboard from "./dashboards/queues";
import replicationDashboard from "./dashboards/replication";
import requestsDashboard from "./dashboards/requests";
import runtimeDashboard from "./dashboards/runtime";
import sqlDashboard from "./dashboards/sql";
import storageDashboard from "./dashboards/storage";
import ttlDashboard from "./dashboards/ttl";
import ClusterSummaryBar from "./summaryBar";

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
  networking: {
    label: "Networking",
    component: networkingDashboard,
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
    label: "Physical Cluster Replication",
    component: crossClusterReplicationDashboard,
    isKvDashboard: true,
  },
  logicalDataReplication: {
    label: "Logical Data Replication",
    component: logicalDataReplicationDashboard,
    isKvDashboard: true,
  },
};

const defaultDashboard = "overview";

const dashboardDropdownOptions = map(dashboards, (dashboard, key) => {
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
  tenantOptions: DropdownOption[];
  currentTenant: string | null;
};

type MapDispatchToProps = {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  refreshNodeSettings: typeof refreshSettings;
  refreshTenantsList: typeof refreshTenantsList;
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

  setClusterPath = (key: string, selected: DropdownOption) => {
    const { match, history } = this.props;
    const { value } = selected;
    const nodeID = getMatchParamByName(match, nodeIDAttr) || "";
    const dashName = getMatchParamByName(match, dashboardNameAttr) || "";
    const tenantName = getMatchParamByName(match, tenantNameAttr) || "";
    const nodeMatchParam = (val: string): string =>
      val === "" ? "/cluster" : `/node/${val}`;
    const tenantMatchParam = (val: string): string =>
      val === "" ? "" : `/tenant/${val}`;
    let path = "/metrics/";
    switch (key) {
      case "dashboard":
        path += value + nodeMatchParam(nodeID) + tenantMatchParam(tenantName);
        break;
      case "node":
        path += dashName + nodeMatchParam(value) + tenantMatchParam(tenantName);
        break;
      default:
        path += dashName + nodeMatchParam(nodeID) + tenantMatchParam(value);
        break;
    }
    history.push(path);
  };

  componentDidMount() {
    this.refresh();
    // settings won't change frequently so it's safe to request one
    // when page is loaded.
    this.props.refreshNodeSettings();
    if (isSystemTenant(this.props.currentTenant)) {
      this.props.refreshTenantsList();
    }
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
      tenantOptions,
      currentTenant,
    } = this.props;
    const canViewKvGraphs =
      getDataFromServer().FeatureFlags.can_view_kv_metric_dashboards;
    const { showLowResolutionAlert, showDeletedDataAlert } = this.state;
    let selectedDashboard = getMatchParamByName(match, dashboardNameAttr);
    if (dashboards[selectedDashboard].isKvDashboard && !canViewKvGraphs) {
      selectedDashboard = defaultDashboard;
    }
    const dashboard = has(dashboards, selectedDashboard)
      ? selectedDashboard
      : defaultDashboard;

    const selectedNode = getMatchParamByName(match, nodeIDAttr) || "";
    const nodeSources = selectedNode !== "" ? [selectedNode] : null;
    const selectedTenant = isSystemTenant(currentTenant)
      ? getMatchParamByName(match, tenantNameAttr) || ""
      : undefined;
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
      tenantSource: selectedTenant,
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
    const graphComponents = map(graphs, (graph, idx) => {
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
      .filter(option => canViewKvGraphs || !option.isKvDashboard);

    return (
      <div style={{ paddingBottom }}>
        <Helmet title={"Metrics"} />
        <h3 className="base-heading">Metrics</h3>
        <PageConfig>
          {/* By default, `tenantOptions` will have a length of 2 for
          "All" and "system" tenant. We should omit showing the
          dropdown in those cases */}
          {isSystemTenant(currentTenant) &&
            containsApplicationTenants(tenantOptions) && (
              <PageConfigItem>
                <Dropdown
                  title="Virtual Cluster"
                  options={tenantOptions}
                  selected={selectedTenant}
                  onChange={selection =>
                    this.setClusterPath("tenant", selection)
                  }
                />
              </PageConfigItem>
            )}
          <PageConfigItem>
            <Dropdown
              title="Graph"
              options={nodeDropdownOptions}
              selected={selectedNode}
              onChange={selection => this.setClusterPath("node", selection)}
            />
          </PageConfigItem>
          <PageConfigItem>
            <Dropdown
              title="Dashboard"
              options={filteredDropdownOptions}
              selected={dashboard}
              onChange={selection =>
                this.setClusterPath("dashboard", selection)
              }
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
              <ClusterSummaryBar
                nodeSources={nodeSources}
                tenantSource={selectedTenant}
              />
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
  state => nodeDisplayNameByIDSelector(state),
  livenessStatusByNodeIDSelector,
  (nodeIds, nodeDisplayNameByID, livenessStatusByNodeID): DropdownOption[] => {
    const base = [{ value: "", label: "Cluster" }];
    return base.concat(
      nodeIds
        .filter(
          id =>
            livenessStatusByNodeID[id] !==
            LivenessStatus.NODE_STATUS_DECOMMISSIONED,
        )
        .map(id => ({
          value: id.toString(),
          label: nodeDisplayNameByID[id],
        })),
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
  nodeDisplayNameByID: nodeDisplayNameByIDSelectorWithoutAddress(state),
  tenantOptions: tenantDropdownOptions(state),
  currentTenant: getCookieValue("tenant"),
});

const mapDispatchToProps: MapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
  refreshNodeSettings: refreshSettings,
  refreshTenantsList,
  hoverOn,
  hoverOff,
  setMetricsFixedWindow: setMetricsFixedWindow,
  setTimeScale: setTimeScale,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(NodeGraphs),
);
