import * as React from "react";
import _ = require("lodash");
import * as ReactPaginate from "react-paginate";
import { Link } from "react-router";
import { connect } from "react-redux";

import { refreshRaft } from "../redux/raft";
import { ToolTip } from "../components/toolTip";

/******************************
 *   RAFT RANGES MAIN COMPONENT
 */

const RANGES_PER_PAGE = 100;

/**
 * RangesMainData are the data properties which should be passed to the RangesMain
 * container.
 */
interface RangesMainData {
  // A list of store statuses to display, which are possibly sorted according to
  // sortSetting.
  rangeStatuses: cockroach.server.RaftDebugResponse;
}

/**
 * RangesMainActions are the action dispatchers which should be passed to the
 * RangesMain container.
 */
interface RangesMainActions {
  // Call if the ranges statuses are stale and need to be refreshed.
  refreshRaft(): void;
}

interface RangesMainState {
  showState?: boolean;
  showReplicas?: boolean;
  showPending?: boolean;
  showOnlyErrors?: boolean;
  pageNum?: number;
  offset?: number;
}

/**
 * RangesMainProps is the type of the props object that must be passed to
 * RangesMain component.
 */
type RangesMainProps = RangesMainData & RangesMainActions;

/**
 * Renders the main content of the help us page.
 */
class RangesMain extends React.Component<RangesMainProps, RangesMainState> {

  state: RangesMainState = {
    showState: true,
    showReplicas: true,
    showPending: true,
    showOnlyErrors: false,
    offset: 0,
  };

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshRaft();
  }

  componentWillReceiveProps(props: RangesMainProps) {
    // Refresh ranges when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    props.refreshRaft();
  }

  renderPagination(pageNum: number): React.ReactNode {
    return <ReactPaginate previousLabel={"previous"}
      nextLabel={"next"}
      breakLabel={<a href="">...</a>}
      pageNum={pageNum}
      marginPagesDisplayed={2}
      pageRangeDisplayed={5}
      clickCallback={this.handlePageClick.bind(this)}
      containerClassName={"pagination"}
      subContainerClassName={"pages pagination"}
      activeClassName={"active"} />;
  }

  handlePageClick(data: any) {
    let selected = data.selected;
    let offset = Math.ceil(selected * RANGES_PER_PAGE);
    this.setState({ offset });
    window.scroll(0, 0);
  }

  // renderFilterSettings renders the filter settings box.
  renderFilterSettings(): React.ReactNode {
    return <div className="section raft-filters">
      <b>Filters</b>
      <label>
        <input type="checkbox" checked={this.state.showState}
          onChange={() => this.setState({showState: !this.state.showState})} />
        State
      </label>
      <label>
        <input type="checkbox" checked={this.state.showReplicas}
          onChange={() => this.setState({showReplicas: !this.state.showReplicas})} />
        Replicas
      </label>
      <label>
        <input type="checkbox" checked={this.state.showPending}
          onChange={() => this.setState({showPending: !this.state.showPending})} />
        Pending
      </label>
      <label>
        <input type="checkbox" checked={this.state.showOnlyErrors}
          onChange={() => this.setState({showOnlyErrors: !this.state.showOnlyErrors})} />
        Only Error Ranges
      </label>
    </div>;
  }

  render() {
    let statuses = this.props.rangeStatuses;
    let content: React.ReactNode = null;

    if (statuses) {
      // Build list of all nodes for static ordering.
      let nodeIDs: number[] = (_(statuses.ranges) as any).map("nodes").flatten().map("node_id").uniq().value().sort();

      let nodeIDIndex: {[nodeID: number]: number} = {};
      let columns = [<th key={-1}>Range</th>];
      nodeIDs.forEach((id, i) => {
        nodeIDIndex[id] = i + 1;
        columns.push(<th key={i}><Link to={"/nodes/" + id}>Node {id}</Link></th>);
      });

      // Filter ranges and paginate
      let filteredRanges = _.filter(statuses.ranges, (range) => {
        return !this.state.showOnlyErrors || range.errors.length > 0;
      });
      let ranges = filteredRanges.slice(this.state.offset, this.state.offset + RANGES_PER_PAGE);
      let rows: React.ReactNode[][] = [];
      ranges.forEach((range, i) => {
        let hasErrors = range.errors.length > 0;
        let errors = <ul>{_.map(range.errors, (error, j) => {
          return <li key={j}>{error.message}</li>;
          })}</ul>;
        let row = [<td key={-1}>
            {range.range_id.toString()}
            {
              (hasErrors) ? (<span style={{position: "relative"}}>
                <div className="viz-info-icon">
                  <div className="icon-warning" />
                </div>
                <ToolTip title="Errors" text={errors} />
              </span>) : ""
            }
          </td>];
        rows[i] = row;

        // Render each replica into a cell
        range.nodes.forEach((node) => {
          let nodeRange = node.range;
          let replicaNodeIDs = nodeRange.desc.replicas.map((replica) => replica.node_id.toString());
          let index = nodeIDIndex[node.node_id];
          let cell = <td key={index}>
            {(this.state.showState) ? <div>State: {nodeRange.raft_state}</div> : ""}
            {(this.state.showReplicas) ? <div>
              <div>Replica On: {replicaNodeIDs.join(", ")}</div>
              <div>Next Replica ID: {nodeRange.desc.next_replica_id}</div>
            </div> : ""}
            {(this.state.showPending) ? <div>Pending Command Count: {nodeRange.pending_cmds || 0}</div> : ""}
          </td>;
          row[index] = cell;
        });

        // Fill empty spaces in table with td elements.
        for (let j = 1; j <= nodeIDs.length; j++) {
          if (!row[j]) {
            row[j] = <td key={j}></td>;
          }
        }
      });

      // Build the final display table
      content = <div>
        {this.renderFilterSettings()}
        <table>
          <thead><tr>{columns}</tr></thead>
          <tbody>
            {_.values(rows).map((row: React.ReactNode[], i: number) => {
              return <tr key={i}>{row}</tr>;
            })}
          </tbody>
        </table>
        <div className="section">
          {this.renderPagination(Math.ceil(filteredRanges.length / RANGES_PER_PAGE))}
        </div>
      </div>;
    } else {
      content = <div className="section">No results.</div>;
    }
    return <div className="section table">
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
let rangeStatuses = (state: any): cockroach.server.RaftDebugResponse => state.raft.statuses;

// Connect the RangesMain class with our redux store.
let rangesMainConnected = connect(
  (state, ownProps) => {
    return {
      rangeStatuses: rangeStatuses(state),
    };
  },
  {
    refreshRaft: refreshRaft,
  }
)(RangesMain);

export { rangesMainConnected as default };
