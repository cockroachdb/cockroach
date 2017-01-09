import * as React from "react";
import { Link } from "react-router";
import { connect } from "react-redux";
import moment from "moment";

import { nodeSums } from "./nodeGraphs";
import { SummaryBar, SummaryHeadlineStat } from "../components/summaryBar";
import { AdminUIState } from "../redux/state";
import { refreshNodes } from "../redux/apiReducers";
import { setUISetting } from "../redux/ui";
import { SortSetting } from "../components/sortabletable";
import { SortedTable } from "../components/sortedtable";
import { NanoToMilli } from "../util/convert";
import { Bytes } from "../util/format";
import { NodeStatus, MetricConstants, BytesUsed } from  "../util/proto";

// Constant used to store sort settings in the redux UI store.
const UI_NODES_SORT_SETTING_KEY = "nodes/sort_setting";

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const NodeSortedTable = SortedTable as new () => SortedTable<Proto2TypeScript.cockroach.server.status.NodeStatus>;

/**
 * NodesMainData are the data properties which should be passed to the NodesMain
 * container.
 */
interface NodesMainData {
  // Current sort setting for the table, which is passed on to the sorted table
  // component.
  sortSetting: SortSetting;
  // A list of store statuses to display.
  statuses: NodeStatus[];
  // Count of replicas across all nodes.
  replicaCount: number;
  // Total used bytes across all nodes.
  usedBytes: number;
  // Total used memory across all nodes.
  usedMem: number;
  // True if current status results are still valid. Needed so that this
  // component refreshes status query when it becomes invalid.
  statusesValid: boolean;
}

/**
 * NodesMainActions are the action dispatchers which should be passed to the
 * NodesMain container.
 */
interface NodesMainActions {
  // Call if the nodes statuses are stale and need to be refreshed.
  refreshNodes: typeof refreshNodes;
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting: typeof setUISetting;
}

/**
 * NodesMainProps is the type of the props object that must be passed to
 * NodesMain component.
 */
type NodesMainProps = NodesMainData & NodesMainActions;

/**
 * Renders the main content of the nodes page, which is primarily a data table
 * of all nodes.
 */
class NodesMain extends React.Component<NodesMainProps, {}> {
  // Callback when the user elects to change the sort setting.
  changeSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_NODES_SORT_SETTING_KEY, setting);
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshNodes();
  }

  componentWillReceiveProps(props: NodesMainProps) {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    props.refreshNodes();
  }

  totalNodes() {
    if (!this.props.statuses) {
      return 0;
    }
    return this.props.statuses.length;
  }

  render() {
    let { statuses, sortSetting } = this.props;

    return <div>
      {
        // TODO(mrtracy): This currently always links back to the main cluster
        // page, when it should link back to the dashboard previously visible.
      }
      <section className="section parent-link">
        <Link to="/cluster">&lt; Back to Cluster</Link>
      </section>
      <section className="header header--subsection">
        Nodes Overview
      </section>
      <section className="section l-columns">
        <div className="l-columns__left">
          <NodeSortedTable
            data={statuses}
            sortSetting={sortSetting}
            onChangeSortSetting={(setting) => this.changeSortSetting(setting) }
            columns={[
              // Node column - displays the node ID, links to the node-specific page for
              // this node.
              {
                title: "Node",
                cell: (ns) => {
                  let lastUpdate = moment(NanoToMilli(ns.updated_at.toNumber()));
                  let s = staleStatus(lastUpdate);
                  return <div>
                    <Link to={"/nodes/" + ns.desc.node_id}>{ns.desc.address.address_field}</Link>
                    <div className={"icon-circle-filled node-status-icon node-status-icon--" + s}/>
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
                  let startTime = moment(NanoToMilli(ns.started_at.toNumber()));
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
                cell: (ns) => ns.metrics.get(MetricConstants.replicas).toString(),
                sort: (ns) => ns.metrics.get(MetricConstants.replicas),
              },
              // Mem Usage - total memory being used on this node.
              {
                title: "Mem Usage",
                cell: (ns) => Bytes(ns.metrics.get(MetricConstants.rss)),
                sort: (ns) => ns.metrics.get(MetricConstants.rss),
              },
              // Logs - a link to the logs data for this node.
              {
                title: "Logs",
                cell: (ns) => <Link to={"/nodes/" + ns.desc.node_id + "/logs"}>Logs</Link>,
                className: "expand-link",
              },
            ]}/>
        </div>
        <div className="l-columns__right">
          <SummaryBar>
            <SummaryHeadlineStat
              title="Total Nodes"
              tooltip="Total number of nodes in the cluster."
              value={ this.totalNodes() }/>
            <SummaryHeadlineStat
              title={ "Total Bytes" }
              tooltip="The total number of bytes stored across all nodes."
              value={ this.props.usedBytes }
              format={ Bytes } />
            <SummaryHeadlineStat
              title="Total Replicas"
              tooltip="The total number of replicas stored across all nodes."
              value={ this.props.replicaCount }/>
            <SummaryHeadlineStat
              title={ "Total Memory Usage" }
              tooltip="The total amount of memory used across all nodes."
              value={ this.props.usedMem }
              format={ Bytes } />
          </SummaryBar>
        </div>
      </section>
    </div>;
  }
}

/******************************
 *         SELECTORS
 */

// Base selectors to extract data from redux state.
let nodeQueryValid = (state: AdminUIState): boolean => state.cachedData.nodes.valid;
let nodeStatuses = (state: AdminUIState): NodeStatus[] => state.cachedData.nodes.data;
let sortSetting = (state: AdminUIState): SortSetting => state.ui[UI_NODES_SORT_SETTING_KEY] || {};

// Connect the NodesMain class with our redux store.
let nodesMainConnected = connect(
  (state: AdminUIState) => {
    let sums = nodeSums(state);
    return {
      statuses: nodeStatuses(state),
      sortSetting: sortSetting(state),
      statusesValid: nodeQueryValid(state),
      replicaCount: sums.replicas,
      usedBytes: sums.usedBytes,
      usedMem: sums.usedMem,
    };
  },
  {
    refreshNodes: refreshNodes,
    setUISetting: setUISetting,
  }
)(NodesMain);

export { nodesMainConnected as default };

/******************************
 *    Formatting Functions
 */

/**
 * staleStatus returns the "status" of the node depending on how long ago
 * the last status update was received.
 *
 *   healthy <=1min
 *   stale   <1min & <=10min
 *   missing >10min
 */
function staleStatus(lastUpdate: moment.Moment): string {
  if (lastUpdate.isBefore(moment().subtract(10, "minutes"))) {
    return "missing";
  }
  if (lastUpdate.isBefore(moment().subtract(1, "minute"))) {
    return "stale";
  }
  return "healthy";
}
