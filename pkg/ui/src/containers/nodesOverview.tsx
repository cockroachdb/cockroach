import * as React from "react";
import { Link } from "react-router";
import { connect } from "react-redux";
import moment from "moment";
import { createSelector } from "reselect";
import _ from "lodash";

import {
  NodesSummary, nodesSummarySelector, LivenessStatus, deadTimeout,
} from "../redux/nodes";
import { SummaryBar, SummaryHeadlineStat } from "../components/summaryBar";
import { AdminUIState } from "../redux/state";
import { refreshNodes, refreshLiveness } from "../redux/apiReducers";
import { LocalSetting } from "../redux/localsettings";
import { SortSetting } from "../components/sortabletable";
import { SortedTable } from "../components/sortedtable";
import { NanoToMilli, LongToMoment } from "../util/convert";
import { Bytes } from "../util/format";
import { NodeStatus$Properties, MetricConstants, BytesUsed } from  "../util/proto";

const liveNodesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "nodes/live_sort_setting", (s) => s.localSettings,
);

const deadNodesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "nodes/dead_sort_setting", (s) => s.localSettings,
);

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const NodeSortedTable = SortedTable as new () => SortedTable<NodeStatus$Properties>;

/**
 * NodeCategoryListProps are the properties shared by both LiveNodeList and
 * DeadNodeList.
 */
interface NodeCategoryListProps {
  sortSetting: SortSetting;
  setSort: typeof liveNodesSortSetting.set;
  statuses: NodeStatus$Properties[];
  nodesSummary: NodesSummary;
}

/**
 * LiveNodeList displays a sortable table of all "live" nodes, which includes
 * both healthy and suspect nodes. Included is a side-bar with summary
 * statistics for these nodes.
 */
class LiveNodeList extends React.Component<NodeCategoryListProps, {}> {
  render() {
    const { statuses, nodesSummary, sortSetting } = this.props;
    if (!statuses || statuses.length === 0) {
      return null;
    }

    return <div>
      <div className="header header--subsection">
        Live Nodes
      </div>
      <section className="section l-columns">
        <div className="l-columns__left">
          <NodeSortedTable
            data={statuses}
            sortSetting={sortSetting}
            onChangeSortSetting={(setting) => this.props.setSort(setting)}
            columns={[
              // Node ID column.
              {
                title: "ID",
                cell: (ns) => ns.desc.node_id,
                sort: (ns) => ns.desc.node_id,
              },
              // Node address column - displays the node address, links to the
              // node-specific page for this node.
              {
                title: "Address",
                cell: (ns) => {
                  const status = nodesSummary.livenessStatusByNodeID[ns.desc.node_id] || 0;
                  const s = LivenessStatus[status].toLowerCase();
                  const tooltip = (status === LivenessStatus.HEALTHY) ?
                    "This node is currently healthy." :
                    "This node has not recently reported as being live. " +
                    "It may not be functioning correctly, but no automatic action has yet been taken.";
                  return <div>
                    <Link to={"/nodes/" + ns.desc.node_id}>{ns.desc.address.address_field}</Link>
                    <div className={"icon-circle-filled node-status-icon node-status-icon--" + s} title={tooltip} />
                  </div>;
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
                  const startTime = moment(NanoToMilli(ns.started_at.toNumber()));
                  return moment.duration(startTime.diff(moment())).humanize();
                },
                sort: (ns) => ns.started_at,
              },
              // Bytes - displays the total persisted bytes maintained by the node.
              {
                title: "Bytes",
                cell: (ns) => Bytes(BytesUsed(ns)),
                sort: (ns) => BytesUsed(ns),
              },
              // Replicas - displays the total number of replicas on the node.
              {
                title: "Replicas",
                cell: (ns) => ns.metrics[MetricConstants.replicas].toString(),
                sort: (ns) => ns.metrics[MetricConstants.replicas],
              },
              // Mem Usage - total memory being used on this node.
              {
                title: "Mem Usage",
                cell: (ns) => Bytes(ns.metrics[MetricConstants.rss]),
                sort: (ns) => ns.metrics[MetricConstants.rss],
              },
              // Logs - a link to the logs data for this node.
              {
                title: "Logs",
                cell: (ns) => <Link to={"/nodes/" + ns.desc.node_id + "/logs"}>Logs</Link>,
                className: "expand-link",
              },
            ]} />
        </div>
        <div className="l-columns__right">
          <SummaryBar>
            <SummaryHeadlineStat
              title="Total Live Nodes"
              tooltip="Total number of live nodes in the cluster."
              value={statuses.length} />
            <SummaryHeadlineStat
              title={"Total Bytes"}
              tooltip="The total number of bytes stored across all live nodes."
              value={nodesSummary.nodeSums.usedBytes}
              format={Bytes} />
            <SummaryHeadlineStat
              title="Total Replicas"
              tooltip="The total number of replicas stored across all live nodes."
              value={nodesSummary.nodeSums.replicas} />
            <SummaryHeadlineStat
              title={"Total Memory Usage"}
              tooltip="The total amount of memory used across all live nodes."
              value={nodesSummary.nodeSums.usedMem}
              format={Bytes} />
          </SummaryBar>
        </div>
      </section>
    </div>;
  }
}

/**
 * DeadNodeList renders a sortable table of all "dead" nodes on the cluster.
 */
class DeadNodeList extends React.Component<NodeCategoryListProps, {}> {
  render() {
    const { statuses, nodesSummary, sortSetting } = this.props;
    if (!statuses || statuses.length === 0) {
      return null;
    }

    return <div>
      <section className="header header--subsection">
        Dead Nodes
      </section>
      <section className="section l-columns">
        <div className="l-columns__left">
          <NodeSortedTable
            data={statuses}
            sortSetting={sortSetting}
            onChangeSortSetting={(setting) => this.props.setSort(setting)}
            columns={[
              // Node ID column.
              {
                title: "ID",
                cell: (ns) => ns.desc.node_id,
                sort: (ns) => ns.desc.node_id,
              },
              // Node address column - displays the node address, links to the
              // node-specific page for this node.
              {
                title: "Address",
                cell: (ns) => {
                  return <div>
                    <Link to={"/nodes/" + ns.desc.node_id}>{ns.desc.address.address_field}</Link>
                    <div className="icon-circle-filled node-status-icon node-status-icon--dead"
                         title={`This node has not reported as live for over ${deadTimeout.humanize()} and is considered dead.`}
                         />
                  </div>;
                },
                sort: (ns) => ns.desc.node_id,
                // TODO(mrtracy): Consider if there is a better way to use BEM
                // style CSS in cases like this; it is a bit awkward to write out
                // the entire modifier class here, but it might not be better to
                // construct the full BEM class in the table component itself.
                className: "sort-table__cell--link",
              },
              // Down since - displays the time that the node started.
              {
                title: "Downtime",
                cell: (ns) => {
                  const liveness = nodesSummary.livenessByNodeID[ns.desc.node_id];
                  const deadTime = liveness.expiration.wall_time;
                  const deadMoment = LongToMoment(deadTime);
                  return moment.duration(deadMoment.diff(moment())).humanize();
                },
                sort: (ns) => ns.started_at,
              },
            ]} />
        </div>
        <div className="l-columns__right">
          <SummaryBar>
            <SummaryHeadlineStat
              title="Total Dead Nodes"
              tooltip="Total number of dead nodes in the cluster."
              value={statuses.length} />
          </SummaryBar>
        </div>
      </section>
    </div>;
  }
}

// Base selectors to extract data from redux state.
const nodeQueryValid = (state: AdminUIState): boolean => state.cachedData.nodes.valid && state.cachedData.liveness.valid;

/**
 * partitionedStatuses divides the list of node statuses into "live" and "dead".
 */
const partitonedStatuses = createSelector(
  nodesSummarySelector,
  (summary) => {
    const liveOrDead = _.partition(
      summary.nodeStatuses,
      (ns) => summary.livenessStatusByNodeID[ns.desc.node_id] !== LivenessStatus.DEAD,
    );
    return {
      live: liveOrDead[0],
      dead: liveOrDead[1],
    };
  },
);

/**
 * LiveNodesConnected is a redux-connected HOC of LiveNodes.
 */
// tslint:disable-next-line:variable-name
const LiveNodesConnected = connect(
  (state: AdminUIState) => {
    const statuses = partitonedStatuses(state);
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
 * DeadNodesConnected is a redux-connected HOC of DeadNodes.
 */
// tslint:disable-next-line:variable-name
const DeadNodesConnected = connect(
  (state: AdminUIState) => {
    const statuses = partitonedStatuses(state);
    return {
      sortSetting: deadNodesSortSetting.selector(state),
      statuses: statuses.dead,
      nodesSummary: nodesSummarySelector(state),
    };
  },
  {
    setSort: deadNodesSortSetting.set,
  },
)(DeadNodeList);

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
  statusesValid: boolean;
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
    return <div>
      <section className="section parent-link">
        <Link to="/cluster">&lt; Back to Cluster</Link>
      </section>
      <LiveNodesConnected/>
      <DeadNodesConnected/>
    </div>;
  }
}

/**
 * nodesMainConnected is a redux-connected HOC of NodesMain.
 */
const nodesMainConnected = connect(
  (state: AdminUIState) => {
    return {
      statusesValid: nodeQueryValid(state),
    };
  },
  {
    refreshNodes,
    refreshLiveness,
  },
)(NodesMain);

export { nodesMainConnected as default };
