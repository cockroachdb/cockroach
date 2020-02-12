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
import { Link } from "react-router-dom";
import { connect } from "react-redux";
import moment, { Moment } from "moment";
import { createSelector } from "reselect";
import _ from "lodash";

import {
  LivenessStatus,
  nodeCapacityStats,
  nodesSummarySelector,
  partitionedStatuses,
  selectNodesSummaryValid,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { refreshNodes, refreshLiveness } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { LongToMoment } from "src/util/convert";
import { INodeStatus, MetricConstants } from "src/util/proto";
import { ColumnsConfig, Table, Text, TextTypes, Tooltip } from "src/components";
import { Percentage } from "src/util/format";
import { FixLong } from "src/util/fixLong";

import TableSection from "./tableSection";
import "./nodes.styl";

const liveNodesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "nodes/live_sort_setting", (s) => s.localSettings,
);

const decommissionedNodesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "nodes/decommissioned_sort_setting", (s) => s.localSettings,
);

// AggregatedNodeStatus indexes have to be greater than LivenessStatus indexes
// for correct sorting in the table.
enum AggregatedNodeStatus {
  LIVE = 6,
  WARNING = 7,
}

// Represents the aggregated dataset with possibly nested items
// for table view. Note: table columns do not match exactly to fields,
// instead, column values are computed based on these fields.
// It is required to reduce computation for top level (grouped) fields,
// and to allow sorting functionality with specific rather then on column value.
export interface NodeStatusRow {
  key: string;
  nodeId?: number;
  nodeName?: string;
  region: string;
  nodesCount?: number;
  uptime?: string;
  replicas: number;
  usedCapacity: number;
  availableCapacity: number;
  usedMemory: number;
  availableMemory: number;
  version?: string;
  /*
  * status is a union of Node statuses and two artificial statuses
  * used to represent the status of top-level grouped items.
  * If all nested nodes have Live status then the current item has Ready status.
  * Otherwise, it has Warning status.
  * */
  status: LivenessStatus | AggregatedNodeStatus;
  children?: Array<NodeStatusRow>;
}

interface DecommissionedNodeStatusRow {
  key: string;
  nodeId: number;
  nodeName: string;
  region: string;
  status: LivenessStatus;
  decommissionedDate: Moment;
}

/**
 * NodeCategoryListProps are the properties shared by both LiveNodeList and
 * NotLiveNodeList.
 */
interface NodeCategoryListProps {
  sortSetting: SortSetting;
  setSort: typeof liveNodesSortSetting.set;
}

interface LiveNodeListProps extends NodeCategoryListProps {
  dataSource: NodeStatusRow[];
  nodesCount: number;
  regionsCount: number;
}

interface DecommissionedNodeListProps extends NodeCategoryListProps {
  dataSource: DecommissionedNodeStatusRow[];
  isCollapsible: boolean;
}

const getStatusDescription = (status: LivenessStatus) => {
  switch (status) {
    case LivenessStatus.LIVE:
      return "This node is currently healthy.";
    case LivenessStatus.DECOMMISSIONING:
      return `This node is in the process of being decommissioned.
       It may take some time to transfer the data to other nodes.
       When finished, it will appear below as a decommissioned node.`;
    default:
      return "This node has not recently reported as being live. " +
        "It may not be functioning correctly, but no automatic action has yet been taken.";
  }
};

/**
 * LiveNodeList displays a sortable table of all "live" nodes, which includes
 * both healthy and suspect nodes. Included is a side-bar with summary
 * statistics for these nodes.
 */
export class NodeList extends React.Component<LiveNodeListProps> {

  readonly columns: ColumnsConfig<NodeStatusRow> = [
    {
      key: "region",
      title: "nodes",
      render: (_text, record) => {
        if (!!record.nodeId) {
          return (
            <React.Fragment>
              <Link className="nodes-table__link" to={`/node/${record.nodeId}`}>
                <Text textType={TextTypes.BodyStrong}>{`N${record.nodeId} `}</Text>
                <Text>{record.nodeName}</Text>
              </Link>
            </React.Fragment>);
        } else {
          // Top level grouping item does not have nodeId
          return (
            <React.Fragment>
              <Text>{record.region}</Text>
            </React.Fragment>);
        }
      },
      sorter: (a, b) => {
        if (!_.isUndefined(a.nodeId) && !_.isUndefined(b.nodeId)) { return 0; }
        if (a.region < b.region) { return -1; }
        if (a.region > b.region) { return 1; }
        return 0;
      },
      className: "column--border-right",
      width: "20%",
    },
    {
      key: "nodesCount",
      title: "node count",
      sorter: (a, b) => {
        if (_.isUndefined(a.nodesCount) || _.isUndefined(b.nodesCount)) { return 0; }
        if (a.nodesCount < b.nodesCount) { return -1; }
        if (a.nodesCount > b.nodesCount) { return 1; }
        return 0;
      },
      render: (_text, record) => record.nodesCount,
      sortDirections: ["ascend", "descend"],
      className: "column--align-right",
      width: "10%",
    },
    {
      key: "uptime",
      dataIndex: "uptime",
      title: "uptime",
      sorter: true,
      className: "column--align-right",
      width: "10%",
    },
    {
      key: "replicas",
      dataIndex: "replicas",
      title: "replicas",
      sorter: true,
      className: "column--align-right",
      width: "10%",
    },
    {
      key: "capacityUse",
      title: "capacity use",
      render: (_text, record) => Percentage(record.usedCapacity, record.availableCapacity),
      sorter: (a, b) =>
        a.usedCapacity / a.availableCapacity - b.usedCapacity / b.availableCapacity,
      className: "column--align-right",
      width: "10%",
    },
    {
      key: "memoryUse",
      title: "memory use",
      render: (_text, record) => Percentage(record.usedMemory, record.availableMemory),
      sorter: (a, b) =>
        a.usedMemory / a.availableMemory - b.usedMemory / b.availableMemory,
      className: "column--align-right",
      width: "10%",
    },
    {
      key: "version",
      dataIndex: "version",
      title: "version",
      sorter: true,
      width: "10%",
      ellipsis: true,
    },
    {
      key: "status",
      render: (_text, record) => {
        let status: string;
        let tooltipText: string;

        switch (record.status) {
          case AggregatedNodeStatus.LIVE:
          case AggregatedNodeStatus.WARNING:
            status = _.capitalize(AggregatedNodeStatus[record.status]);
            break;
          default:
            status = _.capitalize(LivenessStatus[record.status]);
            tooltipText = getStatusDescription(record.status);
            break;
        }
        return (
          <Text
            className={`status-column status-column--color-${status.toLowerCase()}`}
            textType={TextTypes.BodyStrong}>
            { tooltipText ? (<Tooltip title={tooltipText}>{status}</Tooltip>) : status }
          </Text>
        );
      },
      title: "status",
      sorter: (a, b) => a.status - b.status,
      width: "15%",
    },
    {
      key: "logs",
      title: "",
      render: (_text, record) => record.nodeId && (
        <div className="cell--show-on-hover nodes-table__link">
          <Link to={`/node/${record.nodeId}/logs`}>Logs</Link>
        </div>),
      width: "5%",
    },
  ];

  render() {
    const { nodesCount, regionsCount } = this.props;
    let columns = this.columns;
    let dataSource = this.props.dataSource;

    // Remove "Nodes Count" column If nodes are not partitioned by regions,
    if (regionsCount === 1) {
      columns = columns.filter(column => column.key !== "nodesCount");
      dataSource = _.head(dataSource).children;
    }
    return (
      <div className="nodes-overview__panel">
        <TableSection
          id={`nodes-overview__live-nodes`}
          title={`Nodes (${nodesCount})`}
          className="embedded-table">
          <Table dataSource={dataSource} columns={columns} tableLayout="fixed" />
        </TableSection>
      </div>
    );
  }
}

/**
 * DecommissionedNodeList renders a view with a table for recently "decommissioned"
 * nodes on a link on a full list of decommissioned nodes.
 */
class DecommissionedNodeList extends React.Component<DecommissionedNodeListProps> {
  columns: ColumnsConfig<DecommissionedNodeStatusRow> = [
    {
      key: "nodes",
      title: "decommissioned nodes",
      render: (_text, record) => (
        <Link className="nodes-table__link" to={`/node/${record.nodeId}`}>
          <Text textType={TextTypes.BodyStrong}>{`N${record.nodeId} `}</Text>
          <Text>{record.nodeName}</Text>
        </Link>),
    },
    {
      key: "decommissionedSince",
      title: "decommissioned on",
      render: (_text, record) => record.decommissionedDate.format("LL[ at ]h:mm a"),
    },
    {
      key: "status",
      title: "status",
      render: (_text, record) => {
        const status = _.capitalize(LivenessStatus[record.status]);
        const tooltipText = getStatusDescription(record.status);
        return (
          <Text
            className={`status-column status-column--color-${status.toLowerCase()}`}
            textType={TextTypes.Body}>
            <Tooltip title={tooltipText}>
              {status}
            </Tooltip>
          </Text>
        );
      },
    },
  ];

  render() {
    const { dataSource, isCollapsible } = this.props;
    if (_.isEmpty(dataSource)) {
      return null;
    }

    return (
      <div className="nodes-overview__panel">
        <TableSection
          id={`nodes-overview__decommissioned-nodes`}
          title="Recently Decommissioned Nodes"
          footer={<Link to={`/reports/nodes/history`}>View all decommissioned nodes </Link>}
          isCollapsible={isCollapsible}
          className="embedded-table embedded-table--dense">
          <Table dataSource={dataSource} columns={this.columns} />
        </TableSection>
      </div>
    );
  }
}

const getNodeRegion = (nodeStatus: INodeStatus) => {
  const region = nodeStatus.desc.locality?.tiers?.find(tier => tier.key === "region");
  return region ? region.value : undefined;
};

export const liveNodesTableDataSelector = createSelector(
  partitionedStatuses,
  nodesSummarySelector,
  (statuses, nodesSummary) => {
    const liveStatuses = statuses.live || [];

    // Do not display aggregated category and # of nodes column
    // when `withLocalitiesSetup` is false.
    // const withLocalitiesSetup = liveStatuses.some(getNodeRegion);

    // `data` can be represented as nested or flat structure.
    // In case cluster is geo partitioned or at least one locality is specified:
    // - nodes are grouped by region
    // - top level record contains aggregated information about nodes in current region
    // In case cluster is setup without localities:
    // - it represents a flat structure.
    const data = _.chain(liveStatuses)
      .groupBy(getNodeRegion)
      .map((nodesPerRegion: INodeStatus[], regionKey: string): NodeStatusRow => {
        const nestedRows = nodesPerRegion.map((ns, idx): NodeStatusRow => {
          const { used: usedCapacity, usable: availableCapacity } = nodeCapacityStats(ns);
          return {
            key: `${regionKey}-${idx}`,
            nodeId: ns.desc.node_id,
            nodeName: ns.desc.address.address_field,
            region: getNodeRegion(ns),
            uptime: moment.duration(LongToMoment(ns.started_at).diff(moment())).humanize(),
            replicas: ns.metrics[MetricConstants.replicas],
            usedCapacity,
            availableCapacity,
            usedMemory: ns.metrics[MetricConstants.rss],
            availableMemory: FixLong(ns.total_system_memory).toNumber(),
            version: ns.build_info.tag,
            status: nodesSummary.livenessStatusByNodeID[ns.desc.node_id] || LivenessStatus.LIVE,
          };
        });

        return {
          key: `${regionKey}`,
          region: regionKey,
          nodesCount: nodesPerRegion.length,
          replicas: _.sum(nestedRows.map(nr => nr.replicas)),
          usedCapacity: _.sum(nestedRows.map(nr => nr.usedCapacity)),
          availableCapacity: _.sum(nestedRows.map(nr => nr.availableCapacity)),
          usedMemory: _.sum(nestedRows.map(nr => nr.usedMemory)),
          availableMemory: _.sum(nestedRows.map(nr => nr.availableMemory)),
          status: nestedRows.every(nestedRow => nestedRow.status === LivenessStatus.LIVE) ?
            AggregatedNodeStatus.LIVE : AggregatedNodeStatus.WARNING,
          children: nestedRows,
        };
      })
      .value();

    return data;
  });

export const decommissionedNodesTableDataSelector = createSelector(
  partitionedStatuses,
  nodesSummarySelector,
  (statuses, nodesSummary): DecommissionedNodeStatusRow[] => {
    const decommissionedStatuses = statuses.decommissioned || [];

    const getDecommissionedTime = (nodeId: number) => {
      const liveness = nodesSummary.livenessByNodeID[nodeId];
      if (!liveness) {
        return undefined;
      }
      const deadTime = liveness.expiration.wall_time;
      return LongToMoment(deadTime);
    };

    // DecommissionedNodeList displays 5 most recent nodes.
    const data = _.chain(decommissionedStatuses)
      .orderBy([(ns: INodeStatus) => getDecommissionedTime(ns.desc.node_id)], ["desc"])
      .take(5)
      .map((ns: INodeStatus, idx: number) => {
        return {
          key: `${idx}`,
          nodeId: ns.desc.node_id,
          nodeName: ns.desc.address.address_field,
          region: getNodeRegion(ns),
          status: nodesSummary.livenessStatusByNodeID[ns.desc.node_id],
          decommissionedDate: getDecommissionedTime(ns.desc.node_id),
        };
      })
      .value();
    return data;
  });

/**
 * LiveNodesConnected is a redux-connected HOC of LiveNodeList.
 */
// tslint:disable-next-line:variable-name
const NodesConnected = connect(
  (state: AdminUIState) => {
    const liveNodes = partitionedStatuses(state).live || [];
    const data = liveNodesTableDataSelector(state);
    return {
      sortSetting: liveNodesSortSetting.selector(state),
      dataSource: data,
      nodesCount: liveNodes.length,
      regionsCount: data.length,
    };
  },
  {
    setSort: liveNodesSortSetting.set,
  },
)(NodeList);

/**
 * DecommissionedNodesConnected is a redux-connected HOC of NotLiveNodeList.
 */
// tslint:disable-next-line:variable-name
const DecommissionedNodesConnected = connect(
  (state: AdminUIState) => {
    return {
      sortSetting: decommissionedNodesSortSetting.selector(state),
      dataSource: decommissionedNodesTableDataSelector(state),
      isCollapsible: true,
    };
  },
  {
    setSort: decommissionedNodesSortSetting.set,
  },
)(DecommissionedNodeList);

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
      <div className="nodes-overview">
        <NodesConnected />
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
