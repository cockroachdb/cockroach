// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Badge,
  BadgeProps,
  ColumnsConfig,
  SortSetting,
  Table,
  Timestamp,
  util,
} from "@cockroachlabs/cluster-ui";
import capitalize from "lodash/capitalize";
import flow from "lodash/flow";
import groupBy from "lodash/groupBy";
import head from "lodash/head";
import isEmpty from "lodash/isEmpty";
import isUndefined from "lodash/isUndefined";
import last from "lodash/last";
import map from "lodash/map";
import orderBy from "lodash/orderBy";
import sum from "lodash/sum";
import take from "lodash/take";
import moment, { Moment } from "moment-timezone";
import React from "react";
import { connect } from "react-redux";
import { Link } from "react-router-dom";
import { createSelector } from "reselect";

import { Text, TextTypes, Tooltip } from "src/components";
import { cockroach } from "src/js/protos";
import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import { LocalityTier } from "src/redux/localities";
import { LocalSetting } from "src/redux/localsettings";
import {
  LivenessStatus,
  nodeCapacityStats,
  nodesSummarySelector,
  partitionedStatuses,
  selectNodesSummaryValid,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { FixLong } from "src/util/fixLong";
import { getNodeLocalityTiers } from "src/util/localities";
import { INodeStatus, MetricConstants } from "src/util/proto";

import TableSection from "./tableSection";
import "./nodes.styl";
import {
  CPUsTooltip,
  getNodeStatusDescription,
  getStatusDescription,
  MemoryUseTooltip,
  NodeCountTooltip,
  NodelistCapacityUsageTooltip,
  ReplicasTooltip,
  StatusTooltip,
  UptimeTooltip,
  VersionTooltip,
} from "./tooltips";

import MembershipStatus = cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;
import ILiveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;

const liveNodesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "nodes/live_sort_setting",
  s => s.localSettings,
);

const decommissionedNodesSortSetting = new LocalSetting<
  AdminUIState,
  SortSetting
>("nodes/decommissioned_sort_setting", s => s.localSettings);

// AggregatedNodeStatus indexes have to be greater than LivenessStatus indexes
// for correct sorting in the table.
export enum AggregatedNodeStatus {
  LIVE = 100,
  WARNING = 101,
  DEAD = 102,
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
  region?: string;
  tiers?: LocalityTier[];
  nodesCount?: number;
  uptime?: string;
  replicas: number;
  usedCapacity: number;
  availableCapacity: number;
  usedMemory: number;
  availableMemory: number;
  numCpus: number;
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

const getBadgeTypeByNodeStatus = (
  status: LivenessStatus | AggregatedNodeStatus,
): BadgeProps["status"] => {
  switch (status) {
    case LivenessStatus.NODE_STATUS_UNKNOWN:
      return "warning";
    case LivenessStatus.NODE_STATUS_DEAD:
      return "danger";
    case LivenessStatus.NODE_STATUS_UNAVAILABLE:
      return "warning";
    case LivenessStatus.NODE_STATUS_LIVE:
      return "default";
    case LivenessStatus.NODE_STATUS_DECOMMISSIONING:
      return "warning";
    case LivenessStatus.NODE_STATUS_DECOMMISSIONED:
      return "default";
    case LivenessStatus.NODE_STATUS_DRAINING:
      return "warning";
    case AggregatedNodeStatus.LIVE:
      return "default";
    case AggregatedNodeStatus.WARNING:
      return "warning";
    case AggregatedNodeStatus.DEAD:
      return "danger";
    default:
      return "default";
  }
};

// getLivenessStatusName truncates the prefix for status name to keep only
// status name ("NODE_STATUS_LIVE" -> "LIVE").
export const getLivenessStatusName = (status: LivenessStatus): string => {
  const prefix = "NODE_STATUS_";
  const statusKey = LivenessStatus[status];
  return statusKey.replace(prefix, "");
};

const NodeNameColumn: React.FC<{
  record: NodeStatusRow | DecommissionedNodeStatusRow;
  shouldLink?: boolean;
}> = ({ record, shouldLink = true }) => {
  const columnValue = (
    <>
      <Text>{record.nodeName}</Text>
      <Text textType={TextTypes.BodyStrong}>{` (n${record.nodeId})`}</Text>
    </>
  );

  if (shouldLink) {
    return (
      <Link className="nodes-table__link" to={`/node/${record.nodeId}`}>
        {columnValue}
      </Link>
    );
  }

  return columnValue;
};

const NodeLocalityColumn: React.FC<{ record: NodeStatusRow }> = ({
  record,
}) => {
  return (
    <Text>
      <Tooltip
        placement={"bottom"}
        title={
          <div>
            {record.tiers.map((tier, idx) => (
              <div key={idx}>{`${tier.key} = ${tier.value}`}</div>
            ))}
          </div>
        }
      >
        {record.region}
      </Tooltip>
    </Text>
  );
};

const formatWithPossibleStaleIndicator = (
  text: string,
  record: NodeStatusRow,
): string => {
  if (
    record.status === LivenessStatus.NODE_STATUS_DEAD ||
    record.status === AggregatedNodeStatus.DEAD
  ) {
    return `${text} (stale)`;
  }

  return text;
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
      render: (_text: string, record: NodeStatusRow) => {
        if (record.nodeId) {
          return <NodeNameColumn record={record} />;
        } else {
          return <NodeLocalityColumn record={record} />;
        }
      },
      sorter: (a: NodeStatusRow, b: NodeStatusRow) => {
        if (!isUndefined(a.nodeId) && !isUndefined(b.nodeId)) {
          // If nodeId is defined but regionId is not, this means that there is only
          // a single region. In this case, sort the by nodeId.
          if (isUndefined(a.region) && isUndefined(b.region)) {
            return a.nodeId - b.nodeId;
          }
          return 0;
        }
        if (a.region < b.region) {
          return -1;
        }
        if (a.region > b.region) {
          return 1;
        }
        return 0;
      },
      className: "column--border-right",
      width: "20%",
    },
    {
      key: "nodesCount",
      title: <NodeCountTooltip>Node Count</NodeCountTooltip>,
      sorter: (a: NodeStatusRow, b: NodeStatusRow) => {
        if (isUndefined(a.nodesCount) || isUndefined(b.nodesCount)) {
          return 0;
        }
        return a.nodesCount - b.nodesCount;
      },
      render: (_text: string, record: NodeStatusRow) => record.nodesCount,
      sortDirections: ["ascend", "descend"],
      className: "column--align-right",
      width: "10%",
    },
    {
      key: "uptime",
      dataIndex: "uptime",
      render: formatWithPossibleStaleIndicator,
      title: <UptimeTooltip>Uptime</UptimeTooltip>,
      sorter: false,
      className: "column--align-right",
      width: "10%",
      ellipsis: true,
    },
    {
      key: "replicas",
      dataIndex: "replicas",
      render: formatWithPossibleStaleIndicator,
      title: <ReplicasTooltip>Replicas</ReplicasTooltip>,
      sorter: (a: NodeStatusRow, b: NodeStatusRow) => a.replicas - b.replicas,
      className: "column--align-right",
      width: "10%",
    },
    {
      key: "capacityUsage",
      title: (
        <NodelistCapacityUsageTooltip>
          Capacity Usage
        </NodelistCapacityUsageTooltip>
      ),
      render: (_text: string, record: NodeStatusRow) =>
        formatWithPossibleStaleIndicator(
          util.Percentage(record.usedCapacity, record.availableCapacity),
          record,
        ),
      sorter: (a: NodeStatusRow, b: NodeStatusRow) =>
        a.usedCapacity / a.availableCapacity -
        b.usedCapacity / b.availableCapacity,
      className: "column--align-right",
      width: "10%",
    },
    {
      key: "memoryUse",
      title: <MemoryUseTooltip>Memory Use</MemoryUseTooltip>,
      render: (_text: string, record: NodeStatusRow) =>
        formatWithPossibleStaleIndicator(
          util.Percentage(record.usedMemory, record.availableMemory),
          record,
        ),
      sorter: (a: NodeStatusRow, b: NodeStatusRow) =>
        a.usedMemory / a.availableMemory - b.usedMemory / b.availableMemory,
      className: "column--align-right",
      width: "10%",
    },
    {
      key: "numCpus",
      title: <CPUsTooltip>vCPUs</CPUsTooltip>,
      dataIndex: "numCpus",
      sorter: (a: NodeStatusRow, b: NodeStatusRow) => a.numCpus - b.numCpus,
      className: "column--align-right",
      width: "8%",
    },
    {
      key: "version",
      dataIndex: "version",
      title: <VersionTooltip>Version</VersionTooltip>,
      sorter: false,
      width: "8%",
      ellipsis: true,
    },
    {
      key: "status",
      title: <StatusTooltip>Status</StatusTooltip>,
      render: (_text: string, record: NodeStatusRow) => {
        let badgeText: string;
        let tooltipText: string | JSX.Element;
        let nodeTooltip: string | JSX.Element;

        // single node row
        const badgeType = getBadgeTypeByNodeStatus(record.status);
        switch (record.status) {
          case AggregatedNodeStatus.DEAD:
            badgeText = "warning";
            tooltipText = getStatusDescription(LivenessStatus.NODE_STATUS_DEAD);
            nodeTooltip = getNodeStatusDescription(record.status);
            break;
          case AggregatedNodeStatus.LIVE:
          case AggregatedNodeStatus.WARNING:
            badgeText = AggregatedNodeStatus[record.status];
            nodeTooltip = getNodeStatusDescription(record.status);
            break;
          case LivenessStatus.NODE_STATUS_UNKNOWN:
          case LivenessStatus.NODE_STATUS_UNAVAILABLE:
            badgeText = "suspect";
            tooltipText = getStatusDescription(record.status);
            break;
          default:
            badgeText = getLivenessStatusName(record.status);
            tooltipText = getStatusDescription(record.status);
            break;
        }

        // if aggregated row
        if (!record.nodeId) {
          return (
            <Tooltip title={nodeTooltip}>
              {""}
              <Badge status={badgeType} text={badgeText} />
            </Tooltip>
          );
        }

        return (
          <Badge
            status={badgeType}
            text={<Tooltip title={tooltipText}>{badgeText}</Tooltip>}
          />
        );
      },
      sorter: (a: NodeStatusRow, b: NodeStatusRow) => a.status - b.status,
      width: "13%",
    },
    {
      key: "logs",
      title: <span />,
      render: (_text: string, record: NodeStatusRow) =>
        record.nodeId && (
          <div className="cell--show-on-hover ">
            <Link
              className="nodes-table__link"
              to={`/node/${record.nodeId}/logs`}
            >
              Logs
            </Link>
          </div>
        ),
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
      dataSource = head(dataSource).children;
    }
    return (
      <div className="nodes-overview__panel">
        <TableSection
          id={`nodes-overview__live-nodes`}
          title={`Nodes (${nodesCount})`}
          className="embedded-table"
        >
          <Table
            dataSource={dataSource}
            columns={columns}
            tableLayout="fixed"
            className="nodes-overview__live-nodes-table"
          />
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
      render: (_text: string, record: DecommissionedNodeStatusRow) => (
        <NodeNameColumn record={record} shouldLink={false} />
      ),
    },
    {
      key: "decommissionedSince",
      title: "decommissioned on",
      render: (_text: string, record: DecommissionedNodeStatusRow) => (
        <Timestamp
          time={record.decommissionedDate}
          format={util.DATE_FORMAT_24_TZ}
        />
      ),
    },
    {
      key: "status",
      title: "status",
      render: (_text: string, record: DecommissionedNodeStatusRow) => {
        const badgeText = capitalize(LivenessStatus[record.status]);
        const tooltipText = getStatusDescription(record.status);
        return (
          <Badge
            status="default"
            text={<Tooltip title={tooltipText}>{badgeText}</Tooltip>}
          />
        );
      },
    },
  ];

  render() {
    const { dataSource, isCollapsible } = this.props;
    if (isEmpty(dataSource)) {
      return null;
    }

    return (
      <div className="nodes-overview__panel">
        <TableSection
          id={`nodes-overview__decommissioned-nodes`}
          title="Recently Decommissioned Nodes"
          footer={
            <Link to={`/reports/nodes/history`}>
              View all decommissioned nodes{" "}
            </Link>
          }
          isCollapsible={isCollapsible}
          className="embedded-table embedded-table--dense"
        >
          <Table
            dataSource={dataSource}
            columns={this.columns}
            className="nodes-overview__decommissioned-nodes-table"
          />
        </TableSection>
      </div>
    );
  }
}

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
    const data = flow(
      (statuses: INodeStatus[]) =>
        groupBy(statuses, s =>
          s.desc.locality.tiers.map(tier => tier.value).join("."),
        ),
      statusesByTiers =>
        map(
          statusesByTiers,
          (nodesPerRegion: INodeStatus[], regionKey: string): NodeStatusRow => {
            const nestedRows = nodesPerRegion.map((ns, idx): NodeStatusRow => {
              const { used: usedCapacity, usable: availableCapacity } =
                nodeCapacityStats(ns);
              return {
                key: `${regionKey}-${idx}`,
                nodeId: ns.desc.node_id,
                nodeName: ns.desc.address.address_field,
                uptime: moment
                  .duration(util.LongToMoment(ns.started_at).diff(moment()))
                  .humanize(),
                replicas: ns.metrics[MetricConstants.replicas],
                usedCapacity,
                availableCapacity,
                usedMemory: ns.metrics[MetricConstants.rss],
                availableMemory: FixLong(ns.total_system_memory).toNumber(),
                numCpus: ns.num_cpus,
                version: ns.build_info.tag,
                status:
                  nodesSummary.livenessStatusByNodeID[ns.desc.node_id] ||
                  LivenessStatus.NODE_STATUS_LIVE,
              };
            });

            // Grouped buckets with node statuses contain at least one element.
            // The list of tires and lower level location are the same for every
            // element in the group because grouping is made by string composed
            // from location values.
            const firstNodeInGroup = nodesPerRegion[0];
            const tiers = getNodeLocalityTiers(firstNodeInGroup);
            const lastTier = last(tiers);

            const getLocalityStatus = () => {
              const nodesByStatus = groupBy(
                nestedRows,
                (row: NodeStatusRow) => row.status,
              );

              // Return DEAD status if at least one node is dead;
              if (!isEmpty(nodesByStatus[LivenessStatus.NODE_STATUS_DEAD])) {
                return AggregatedNodeStatus.DEAD;
              }

              // Return WARNING status if at least one node is decommissioning or suspected;
              if (
                !isEmpty(
                  nodesByStatus[LivenessStatus.NODE_STATUS_DECOMMISSIONING],
                ) ||
                !isEmpty(nodesByStatus[LivenessStatus.NODE_STATUS_UNKNOWN]) ||
                !isEmpty(nodesByStatus[LivenessStatus.NODE_STATUS_UNAVAILABLE])
              ) {
                return AggregatedNodeStatus.WARNING;
              }

              return AggregatedNodeStatus.LIVE;
            };

            return {
              key: `${regionKey}`,
              region: lastTier?.value,
              tiers,
              nodesCount: nodesPerRegion.length,
              replicas: sum(nestedRows.map(nr => nr.replicas)),
              usedCapacity: sum(nestedRows.map(nr => nr.usedCapacity)),
              availableCapacity: sum(
                nestedRows.map(nr => nr.availableCapacity),
              ),
              usedMemory: sum(nestedRows.map(nr => nr.usedMemory)),
              availableMemory: sum(nestedRows.map(nr => nr.availableMemory)),
              numCpus: sum(nestedRows.map(nr => nr.numCpus)),
              status: getLocalityStatus(),
              children: nestedRows,
            };
          },
        ),
    )(liveStatuses);

    return data;
  },
);

export const decommissionedNodesTableDataSelector = createSelector(
  nodesSummarySelector,
  (nodesSummary): DecommissionedNodeStatusRow[] => {
    const getDecommissionedTime = (nodeId: number) => {
      const liveness = nodesSummary.livenessByNodeID[nodeId];
      if (!liveness) {
        return undefined;
      }
      const deadTime = liveness.expiration.wall_time;
      return util.LongToMoment(deadTime);
    };

    const decommissionedNodes = Object.values(
      nodesSummary.livenessByNodeID,
    ).filter(liveness => {
      return liveness?.membership === MembershipStatus.DECOMMISSIONED;
    });

    // DecommissionedNodeList displays 5 most recent nodes.
    const data = flow(
      (nodes: ILiveness[]) =>
        orderBy(
          nodes,
          [liveness => getDecommissionedTime(liveness.node_id)],
          ["desc"],
        ),
      nodes => take(nodes, 5),
      nodes =>
        map(nodes, (liveness, idx: number) => {
          const { node_id } = liveness;
          return {
            key: `${idx}`,
            nodeId: node_id,
            nodeName: `${node_id}`,
            status: nodesSummary.livenessStatusByNodeID[node_id],
            decommissionedDate: getDecommissionedTime(node_id),
          };
        }),
    )(decommissionedNodes);
    return data;
  },
);

/**
 * LiveNodesConnected is a redux-connected HOC of LiveNodeList.
 */
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
  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentDidUpdate() {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    this.props.refreshNodes();
    this.props.refreshLiveness();
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
