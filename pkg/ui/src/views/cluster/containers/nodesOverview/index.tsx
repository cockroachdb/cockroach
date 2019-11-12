// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Link } from "react-router";
import { connect } from "react-redux";
import moment from "moment";
import { createSelector } from "reselect";
import _ from "lodash";

import {
  LivenessStatus,
  nodeCapacityStats,
  NodesSummary,
  nodesSummarySelector,
  selectNodesSummaryValid,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { refreshNodes, refreshLiveness } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";
import { LongToMoment } from "src/util/convert";
import { INodeStatus, MetricConstants, BytesUsed } from "src/util/proto";
import { FixLong } from "src/util/fixLong";
import { trustIcon } from "src/util/trust";
import liveIcon from "!!raw-loader!assets/livenessIcons/live.svg";
import suspectIcon from "!!raw-loader!assets/livenessIcons/suspect.svg";
import deadIcon from "!!raw-loader!assets/livenessIcons/dead.svg";
import { cockroach } from "src/js/protos";

import { BytesBarChart } from "./barChart";
import "./nodes.styl";

import NodeLivenessStatus = cockroach.storage.NodeLivenessStatus;

const liveNodesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "nodes/live_sort_setting", (s) => s.localSettings,
);

const deadNodesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "nodes/dead_sort_setting", (s) => s.localSettings,
);

const decommissionedNodesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "nodes/decommissioned_sort_setting", (s) => s.localSettings,
);

class NodeSortedTable extends SortedTable<INodeStatus> {}

/**
 * NodeCategoryListProps are the properties shared by both LiveNodeList and
 * NotLiveNodeList.
 */
interface NodeCategoryListProps {
  sortSetting: SortSetting;
  setSort: typeof liveNodesSortSetting.set;
  statuses: INodeStatus[];
  nodesSummary: NodesSummary;
}

/**
 * LiveNodeList displays a sortable table of all "live" nodes, which includes
 * both healthy and suspect nodes. Included is a side-bar with summary
 * statistics for these nodes.
 */
class LiveNodeList extends React.Component<NodeCategoryListProps, {}> {
  getLivenessIcon(livenessStatus: NodeLivenessStatus) {
    switch (livenessStatus) {
      case NodeLivenessStatus.LIVE:
        return liveIcon;
      case NodeLivenessStatus.DEAD:
        return deadIcon;
      default:
        return suspectIcon;
    }
  }

  render() {
    const { statuses, nodesSummary, sortSetting } = this.props;
    if (!statuses || statuses.length === 0) {
      return null;
    }

    return (
      <div className="embedded-table">
        <section className="section section--heading">
          <h2>Live Nodes</h2>
        </section>
        <NodeSortedTable
          data={statuses}
          sortSetting={sortSetting}
          onChangeSortSetting={(setting) => this.props.setSort(setting)}
          columns={[
            // Node ID column.
            {
              title: "ID",
              cell: (ns) => `n${ns.desc.node_id}`,
              sort: (ns) => ns.desc.node_id,
            },
            // Node address column - displays the node address, links to the
            // node-specific page for this node.
            {
              title: "Address",
              cell: (ns) => {
                const status = nodesSummary.livenessStatusByNodeID[ns.desc.node_id] || LivenessStatus.LIVE;
                const icon = this.getLivenessIcon(status);
                let tooltip: string;
                switch (status) {
                  case LivenessStatus.LIVE:
                    tooltip = "This node is currently healthy.";
                    break;
                  case LivenessStatus.DECOMMISSIONING:
                    tooltip = "This node is currently being decommissioned.";
                    break;
                  default:
                    tooltip = "This node has not recently reported as being live. " +
                      "It may not be functioning correctly, but no automatic action has yet been taken.";
                }
                return (
                  <div className="sort-table__unbounded-column">
                    <span className="node-status-icon" title={tooltip} dangerouslySetInnerHTML={ trustIcon(icon) } />
                    <Link to={`/node/${ns.desc.node_id}`}>{ns.desc.address.address_field}</Link>
                  </div>
                );
              },
              sort: (ns) => ns.desc.node_id,
              // TODO(mrtracy): Consider if there is a better way to use BEM
              // style CSS in cases like this; it is a bit awkward to write out
              // the entire modifier class here, but it might not be better to
              // construct the full BEM class in the table component itself.
              className: "sort-table__cell--link",
            },
            // Started at - displays the time that the node started.
            {
              title: "Uptime",
              cell: (ns) => {
                const startTime = LongToMoment(ns.started_at);
                return moment.duration(startTime.diff(moment())).humanize();
              },
              sort: (ns) => ns.started_at,
              className: "sort-table__cell--right-aligned-stats",
            },
            // Replicas - displays the total number of replicas on the node.
            {
              title: "Replicas",
              cell: (ns) => ns.metrics[MetricConstants.replicas].toString(),
              sort: (ns) => ns.metrics[MetricConstants.replicas],
              className: "sort-table__cell--right-aligned-stat",
            },
            // CPUs - the number of CPUs on this node
            {
              title: "CPUs",
              cell: (ns) => ns.num_cpus,
              className: "sort-table__cell--right-aligned-stat",
            },
            // Used Capacity - displays the total persisted bytes maintained by the node.
            {
              title: "Capacity Usage",
              cell: (ns) => {
                const { usable } = nodeCapacityStats(ns);
                const used = BytesUsed(ns);
                return <BytesBarChart used={used} usable={usable} />;
              },
              sort: (ns) => BytesUsed(ns) / nodeCapacityStats(ns).usable,
            },
            // Mem Usage - total memory being used on this node.
            {
              title: "Mem Usage",
              cell: (ns) => {
                const used = ns.metrics[MetricConstants.rss];
                const available = FixLong(ns.total_system_memory).toNumber();
                return <BytesBarChart used={used} usable={available} />;
              },
              sort: (ns) => ns.metrics[MetricConstants.rss] / FixLong(ns.total_system_memory).toNumber(),
            },
            // Version - the currently running version of cockroach.
            {
              title: "Version",
              cell: (ns) => ns.build_info.tag,
              sort: (ns) => ns.build_info.tag,
            },
            // Logs - a link to the logs data for this node.
            {
              title: "Logs",
              cell: (ns) => <Link to={`/node/${ns.desc.node_id}/logs`}>Logs</Link>,
              className: "expand-link",
            },
          ]} />
      </div>
    );
  }
}

/**
 * NotLiveNodeListProps are the properties of NotLiveNodeList.
 */
interface NotLiveNodeListProps extends NodeCategoryListProps {
  status: LivenessStatus.DECOMMISSIONING | LivenessStatus.DEAD;
}

/**
 * NotLiveNodeList renders a sortable table of all "dead" or "decommissioned"
 * nodes on the cluster.
 */
class NotLiveNodeList extends React.Component<NotLiveNodeListProps, {}> {
  render() {
    const { status, statuses, nodesSummary, sortSetting } = this.props;
    if (!statuses || statuses.length === 0) {
      return null;
    }

    const statusName = _.capitalize(LivenessStatus[status]);

    return (
      <div className="embedded-table">
        <section className="section section--heading">
          <h2>{`${statusName} Nodes`}</h2>
        </section>
        <NodeSortedTable
          data={statuses}
          sortSetting={sortSetting}
          onChangeSortSetting={(setting) => this.props.setSort(setting)}
          columns={[
            // Node ID column.
            {
              title: "ID",
              cell: (ns) => `n${ns.desc.node_id}`,
              sort: (ns) => ns.desc.node_id,
            },
            // Node address column - displays the node address, links to the
            // node-specific page for this node.
            {
              title: "Address",
              cell: (ns) => {
                return (
                  <div>
                    <span className="node-status-icon"
                      title={
                        "This node has not reported as live for a significant period and is considered dead. " +
                        "The cut-off period for dead nodes is configurable as cluster setting " +
                        "'server.time_until_store_dead'"
                      }
                      dangerouslySetInnerHTML={ trustIcon(deadIcon) } />
                    <Link to={`/node/${ns.desc.node_id}`}>{ns.desc.address.address_field}</Link>
                  </div>
                );
              },
              sort: (ns) => ns.desc.node_id,
              // TODO(mrtracy): Consider if there is a better way to use BEM
              // style CSS in cases like this; it is a bit awkward to write out
              // the entire modifier class here, but it might not be better to
              // construct the full BEM class in the table component itself.
              className: "sort-table__cell--link",
            },
            // Down/decommissioned since - displays how long the node has been
            // considered dead.
            {
              title: `${statusName} Since`,
              cell: (ns) => {
                const liveness = nodesSummary.livenessByNodeID[ns.desc.node_id];
                if (!liveness) {
                  return "no information";
                }

                const deadTime = liveness.expiration.wall_time;
                const deadMoment = LongToMoment(deadTime);
                return `${moment.duration(deadMoment.diff(moment())).humanize()} ago`;
              },
              sort: (ns) => {
                const liveness = nodesSummary.livenessByNodeID[ns.desc.node_id];
                return liveness.expiration.wall_time;
              },
            },
          ]} />
      </div>
    );
  }
}

/**
 * partitionedStatuses divides the list of node statuses into "live" and "dead".
 */
const partitionedStatuses = createSelector(
  nodesSummarySelector,
  (summary) => {
    return _.groupBy(
      summary.nodeStatuses,
      (ns) => {
        switch (summary.livenessStatusByNodeID[ns.desc.node_id]) {
          case LivenessStatus.LIVE:
          case LivenessStatus.UNAVAILABLE:
          case LivenessStatus.DECOMMISSIONING:
            return "live";
          case LivenessStatus.DECOMMISSIONED:
            return "decommissioned";
          case LivenessStatus.DEAD:
          default:
            return "dead";
        }
      },
    );
  },
);

/**
 * LiveNodesConnected is a redux-connected HOC of LiveNodeList.
 */
// tslint:disable-next-line:variable-name
const LiveNodesConnected = connect(
  (state: AdminUIState) => {
    const statuses = partitionedStatuses(state);
    return {
      sortSetting: liveNodesSortSetting.selector(state),
      statuses: statuses.live,
      nodesSummary: nodesSummarySelector(state),
    };
  },
  {
    setSort: liveNodesSortSetting.set,
  },
)(LiveNodeList);

/**
 * DeadNodesConnected is a redux-connected HOC of NotLiveNodeList.
 */
// tslint:disable-next-line:variable-name
const DeadNodesConnected = connect(
  (state: AdminUIState) => {
    const statuses = partitionedStatuses(state);
    return {
      sortSetting: deadNodesSortSetting.selector(state),
      status: LivenessStatus.DEAD,
      statuses: statuses.dead,
      nodesSummary: nodesSummarySelector(state),
    };
  },
  {
    setSort: deadNodesSortSetting.set,
  },
)(NotLiveNodeList);

/**
 * DecommissionedNodesConnected is a redux-connected HOC of NotLiveNodeList.
 */
// tslint:disable-next-line:variable-name
const DecommissionedNodesConnected = connect(
  (state: AdminUIState) => {
    const statuses = partitionedStatuses(state);
    return {
      sortSetting: decommissionedNodesSortSetting.selector(state),
      status: LivenessStatus.DECOMMISSIONED,
      statuses: statuses.decommissioned,
      nodesSummary: nodesSummarySelector(state),
    };
  },
  {
    setSort: decommissionedNodesSortSetting.set,
  },
)(NotLiveNodeList);

/**
 * NodesMainProps is the type of the props object that must be passed to
 * NodesMain component.
 */
interface NodesMainProps {
  // Call if the nodes statuses are stale and need to be refreshed.
  refreshNodes: typeof refreshNodes;
  // Call if the liveness statuses are stale and need to be refreshed.
  refreshLiveness: typeof refreshLiveness;
  // True if current status results are still valid. Needed so that this
  // component refreshes status query when it becomes invalid.
  nodesSummaryValid: boolean;
}

/**
 * Renders the main content of the nodes page, which is primarily a data table
 * of all nodes.
 */
class NodesMain extends React.Component<NodesMainProps, {}> {
  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentWillReceiveProps(props: NodesMainProps) {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    props.refreshNodes();
    props.refreshLiveness();
  }

  render() {
    return (
      <div>
        <DeadNodesConnected />
        <LiveNodesConnected />
        <DecommissionedNodesConnected />
      </div>
    );
  }
}

/**
 * NodesMainConnected is a redux-connected HOC of NodesMain.
 */
// tslint:disable-next-line:variable-name
const NodesMainConnected = connect(
  (state: AdminUIState) => {
    return {
      nodesSummaryValid: selectNodesSummaryValid(state),
    };
  },
  {
    refreshNodes,
    refreshLiveness,
  },
)(NodesMain);

export { NodesMainConnected as NodesOverview };
