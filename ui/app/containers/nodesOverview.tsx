import * as React from "react";
import _ = require("lodash");
import { Link } from "react-router";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import * as d3 from "d3";
import * as moment from "moment";

import { AdminUIState } from "../redux/state";
import { refreshNodes } from "../redux/apiReducers";
import { setUISetting } from "../redux/ui";
import { SortableTable, SortableColumn, SortSetting } from "../components/sortabletable";
import { NanoToMilli } from "../util/convert";
import { BytesToUnitValue } from "../util/format";
import { NodeStatus, MetricConstants, TotalCpu, BytesUsed } from  "../util/proto";

// Constant used to store sort settings in the redux UI store.
const UI_NODES_SORT_SETTING_KEY = "nodes/sort_setting";

/******************************
 *      COLUMN DEFINITION
 */

/**
 * NodesTableColumn provides an enumeration value for each column in the nodes table.
 */
enum NodesTableColumn {
  Health = 1,
  NodeID,
  StartedAt,
  Bytes,
  Replicas,
  Connections,
  CPU,
  MemUsage,
  Logs,
}

/**
 * NodesColumnDescriptor is used to describe metadata about an individual column
 * in the Nodes table.
 */
interface NodeColumnDescriptor {
  // Enumeration key to distinguish this column from others.
  key: NodesTableColumn;
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (ns: NodeStatus) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // NodeStatus. This will be used to sort the table according to the data in
  // this column.
  sort?: (ns: NodeStatus) => any;
  // Function that generates a "rollup" value for this column from all statuses
  // in a collection. This is used to display an appropriate "total" value for
  // each column.
  rollup?: (ns: NodeStatus[]) => React.ReactNode;
  // className to be applied to the td elements
  className?: string;
}

/**
 * columnDescriptors describes all columns that appear in the nodes table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let columnDescriptors: NodeColumnDescriptor[] = [
  // Health column - a simple red/yellow/green status indicator.
  {
    key: NodesTableColumn.Health,
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
    key: NodesTableColumn.NodeID,
    title: "Node",
    cell: (ns) => <Link to={"/nodes/" + ns.desc.node_id}>{ns.desc.address.address_field}</Link>,
    sort: (ns) => ns.desc.node_id,
    rollup: (rows) => {
      interface StatusTotals {
        missing?: number;
        stale?: number;
        healthy?: number;
      }
      let statuses: StatusTotals = _.countBy(rows, (row) => staleStatus(moment(NanoToMilli(row.updated_at.toNumber()))));

      return <div className="node-counts">
        <span className="healthy">{statuses.healthy || 0}</span>
        <span>/</span>
        <span className="stale">{statuses.stale || 0}</span>
        <span>/</span>
        <span className="missing">{statuses.missing || 0}</span>
      </div>;
    },
    className: "expand-link",
  },
  // Started at - displays the time that the node started.
  {
    key: NodesTableColumn.StartedAt,
    title: "Started",
    cell: (ns) => {
      return moment(NanoToMilli(ns.started_at.toNumber())).fromNow();
    },
    sort: (ns) => ns.started_at,
  },
  // Bytes - displays the total persisted bytes maintained by the node.
  {
    key: NodesTableColumn.Bytes,
    title: "Bytes",
    cell: (ns) => formatBytes(BytesUsed(ns)),
    sort: (ns) => BytesUsed(ns),
    rollup: (rows) => formatBytes(_.sumBy(rows, (row) => BytesUsed(row))),
  },
  // Replicas - displays the total number of replicas on the node.
  {
    key: NodesTableColumn.Replicas,
    title: "Replicas",
    cell: (ns) => ns.metrics.get(MetricConstants.replicas).toString(),
    sort: (ns) => ns.metrics.get(MetricConstants.replicas),
    rollup: (rows) => _.sumBy(rows, (row) => row.metrics.get(MetricConstants.replicas)).toString(),
  },
  // Connections - the total number of open connections on the node.
  {
    key: NodesTableColumn.Connections,
    title: "Connections",
    cell: (ns) => ns.metrics.get(MetricConstants.sqlConns).toString(),
    sort: (ns) => ns.metrics.get(MetricConstants.sqlConns),
    rollup: (rows) => _.sumBy(rows, (row) => row.metrics.get(MetricConstants.sqlConns)).toString(),
  },
  // CPU - total CPU being used on this node.
  {
    key: NodesTableColumn.CPU,
    title: "CPU Usage",
    cell: (ns) => d3.format(".2%")(TotalCpu(ns)),
    sort: (ns) => TotalCpu(ns),
    rollup: (rows) => d3.format(".2%")(_.sumBy(rows, (row) => TotalCpu(row))),
  },
  // Mem Usage - total memory being used on this node.
  {
    key: NodesTableColumn.MemUsage,
    title: "Mem Usage",
    cell: (ns) => formatBytes(ns.metrics.get(MetricConstants.rss)),
    sort: (ns) => ns.metrics.get(MetricConstants.rss),
    rollup: (rows) => formatBytes(_.sumBy(rows, (row) => row.metrics.get(MetricConstants.rss))),
  },
  // Logs - a link to the logs data for this node.
  {
    key: NodesTableColumn.Logs,
    title: "Logs",
    cell: (ns) => <Link to={"/nodes/" + ns.desc.node_id + "/logs"}>Logs</Link>,
    className: "expand-link",
  },
];

/**
 * NodeStatusRollups contains rollups for each column in a table, organized by
 * key.
 */
interface NodeStatusRollups {
  [key: number]: React.ReactNode;
}

/******************************
 *   NODES MAIN COMPONENT
 */

/**
 * NodesMainData are the data properties which should be passed to the NodesMain
 * container.
 */
interface NodesMainData {
  // Current sort setting for the table. Incoming rows will already be sorted
  // according to this setting.
  sortSetting: SortSetting;
  // A list of store statuses to display, which are possibly sorted according to
  // sortSetting.
  sortedStatuses: NodeStatus[];
  // Per-column rollups computed for the current statuses.
  statusRollups: NodeStatusRollups;
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
  /**
   * columns is a selector which computes the input Columns to our data table,
   * based our columnDescriptors and the current sorted data
   */
  columns = createSelector(
    (props: NodesMainProps) => props.sortedStatuses,
    (props: NodesMainProps) => props.statusRollups,
    (statuses: NodeStatus[], rollups: NodeStatusRollups) => {
      return _.map(columnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(statuses[index]),
          sortKey: cd.sort ? cd.key : undefined,
          rollup: rollups[cd.key],
          className: cd.className,
        };
      });
    });

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
    let { sortedStatuses: statuses, sortSetting } = this.props;
    let content: React.ReactNode = null;

    if (statuses) {
      content = <SortableTable count={statuses.length}
                       sortSetting={sortSetting}
                       onChangeSortSetting={(setting) => this.changeSortSetting(setting)}>
        {this.columns(this.props)}
      </SortableTable>;
    } else {
      content = <div>No results.</div>;
    }

    return <div className="section table node-overview">
      { this.props.children }
      <div className="stats-table">
        { content }
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

// Selector which sorts statuses according to current sort setting.
let sortFunctionLookup = _.reduce(
  columnDescriptors,
  (memo, cd) => {
    if (cd.sort) {
      memo[cd.key] = cd.sort;
    }
    return memo;
  },
  {} as {[key: number]: (ns: NodeStatus) => any}
);

let sortedStatuses = createSelector(
  nodeStatuses,
  sortSetting,
  (statuses, sort) => {
    if (!sort) {
      return statuses;
    }
    let sortFn = sortFunctionLookup[sort.sortKey];
    if (!sortFn) {
      return statuses;
    }
    let result = _.chain(statuses);
    result = result.sortBy(sortFn);
    if (sort.ascending) {
      result = result.reverse();
    }
    return result.value();
  });

// Selector which computes status rollups for the current node status set.
let rollupStatuses = createSelector(
  nodeStatuses,
  (statuses) => {
    let rollups: NodeStatusRollups = {};
    _.map(columnDescriptors, (c) => {
      if (c.rollup) {
        rollups[c.key] = c.rollup(statuses);
      }
    });
    return rollups;
  });

// Connect the NodesMain class with our redux store.
let nodesMainConnected = connect(
  (state: AdminUIState) => {
    return {
      sortedStatuses: sortedStatuses(state),
      statusRollups: rollupStatuses(state),
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
