import * as React from "react";
import _ from "lodash";
import { Link } from "react-router";
import { connect } from "react-redux";
import * as d3 from "d3";
import moment from "moment";

import { AdminUIState } from "../redux/state";
import { refreshNodes } from "../redux/apiReducers";
import { setUISetting } from "../redux/ui";
import { SortSetting } from "../components/sortabletable";
import { SortedTable } from "../components/sortedtable";
import { NanoToMilli } from "../util/convert";
import { BytesToUnitValue } from "../util/format";
import { NodeStatus, MetricConstants, TotalCpu, BytesUsed } from  "../util/proto";

// Constant used to store sort settings in the redux UI store.
const UI_NODES_SORT_SETTING_KEY = "nodes/sort_setting";

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const NodeSortedTable = SortedTable as new () => SortedTable<cockroach.server.status.NodeStatus>;

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

  render() {
    let { statuses, sortSetting } = this.props;

    return <div className="section table node-overview">
      { this.props.children }
      <div className="stats-table">
        <NodeSortedTable
          data={statuses}
          sortSetting={sortSetting}
          onChangeSortSetting={(setting) => this.changeSortSetting(setting) }
          columns={[
            // Health column - a simple red/yellow/green status indicator.
            {
              title: "",
              cell: (ns) => {
                let lastUpdate = moment(NanoToMilli(ns.updated_at.toNumber()));
                let s = staleStatus(lastUpdate);
                return <div className={"status icon-circle-filled " + s}/>;
              },
            },
            // Node column - displays the node ID, links to the node-specific page for
            // this node.
            {
              title: "Node",
              cell: (ns) => <Link to={"/nodes/" + ns.desc.node_id}>{ns.desc.address.address_field}</Link>,
              sort: (ns) => ns.desc.node_id,
              rollup: (rows) => {
                interface StatusTotals {
                  missing?: number;
                  stale?: number;
                  healthy?: number;
                }
                let healthTotals: StatusTotals = _.countBy(rows, (row) => staleStatus(moment(NanoToMilli(row.updated_at.toNumber()))));

                return <div className="node-counts">
                  <span className="healthy">{healthTotals.healthy || 0}</span>
                  <span>/</span>
                  <span className="stale">{healthTotals.stale || 0}</span>
                  <span>/</span>
                  <span className="missing">{healthTotals.missing || 0}</span>
                </div>;
              },
              className: "expand-link",
            },
            // Started at - displays the time that the node started.
            {
              title: "Started",
              cell: (ns) => {
                return moment(NanoToMilli(ns.started_at.toNumber())).fromNow();
              },
              sort: (ns) => ns.started_at,
            },
            // Bytes - displays the total persisted bytes maintained by the node.
            {
              title: "Bytes",
              cell: (ns) => formatBytes(BytesUsed(ns)),
              sort: (ns) => BytesUsed(ns),
              rollup: (rows) => formatBytes(_.sumBy(rows, (row) => BytesUsed(row))),
            },
            // Replicas - displays the total number of replicas on the node.
            {
              title: "Replicas",
              cell: (ns) => ns.metrics.get(MetricConstants.replicas).toString(),
              sort: (ns) => ns.metrics.get(MetricConstants.replicas),
              rollup: (rows) => _.sumBy(rows, (row) => row.metrics.get(MetricConstants.replicas)).toString(),
            },
            // Connections - the total number of open connections on the node.
            {
              title: "Connections",
              cell: (ns) => ns.metrics.get(MetricConstants.sqlConns).toString(),
              sort: (ns) => ns.metrics.get(MetricConstants.sqlConns),
              rollup: (rows) => _.sumBy(rows, (row) => row.metrics.get(MetricConstants.sqlConns)).toString(),
            },
            // CPU - total CPU being used on this node.
            {
              title: "CPU Usage",
              cell: (ns) => d3.format(".2%")(TotalCpu(ns)),
              sort: (ns) => TotalCpu(ns),
              rollup: (rows) => d3.format(".2%")(_.sumBy(rows, (row) => TotalCpu(row))),
            },
            // Mem Usage - total memory being used on this node.
            {
              title: "Mem Usage",
              cell: (ns) => formatBytes(ns.metrics.get(MetricConstants.rss)),
              sort: (ns) => ns.metrics.get(MetricConstants.rss),
              rollup: (rows) => formatBytes(_.sumBy(rows, (row) => row.metrics.get(MetricConstants.rss))),
            },
            // Logs - a link to the logs data for this node.
            {
              title: "Logs",
              cell: (ns) => <Link to={"/nodes/" + ns.desc.node_id + "/logs"}>Logs</Link>,
              className: "expand-link",
            },
          ]}/>
      </div>
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
    return {
      statuses: nodeStatuses(state),
      sortSetting: sortSetting(state),
      statusesValid: nodeQueryValid(state),
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

/**
 * formatBytes creates a human-readable representation of the given byte
 * count, converting to an appropriate prefix unit and appending a unit symbol
 * to the amount.
 */
function formatBytes(bytes: number): React.ReactNode {
  let b = BytesToUnitValue(bytes);
  return <div>
    {b.value.toFixed(1)}
    <span className="units">{b.units}</span>
  </div>;
}
