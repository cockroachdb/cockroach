import * as _ from "lodash";
import * as React from "react";
import * as ReactPaginate from "react-paginate";
import { Link } from "react-router";
import { connect } from "react-redux";

import { AdminUIState } from "../redux/state";
import { refreshRaft } from "../redux/apiReducers";
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
  rangeStatuses: cockroach.server.serverpb.RaftDebugResponse;
}

/**
 * RangesMainActions are the action dispatchers which should be passed to the
 * RangesMain container.
 */
interface RangesMainActions {
  // Call if the ranges statuses are stale and need to be refreshed.
  refreshRaft: typeof refreshRaft;
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
 * Renders the main content of the raft ranges page, which is primarily a data
 * table of all ranges and their replicas.
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
    if (!props.rangeStatuses) {
      props.refreshRaft();
    }
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
      let nodeIDs = _(statuses.ranges.map).flatMap("value.nodes").map("node_id").uniq().value().sort() as number[];

      let nodeIDIndex: {[nodeID: number]: number} = {};
      let columns = [<th key={-1}>Range</th>];
      nodeIDs.forEach((id, i) => {
        nodeIDIndex[id] = i + 1;
        columns.push(<th key={i}><Link to={"/nodes/" + id}>Node {id}</Link></th>);
      });

      // Filter ranges and paginate
      let filteredRanges = _.filter(statuses.ranges.map, (range) => {
        return !this.state.showOnlyErrors || range.value.errors.length > 0;
      });
      let offset = this.state.offset;
      if (this.state.offset > filteredRanges.length) {
        offset = 0;
      }
      let ranges = filteredRanges.slice(offset, offset + RANGES_PER_PAGE);
      let rows: React.ReactNode[][] = [];
      ranges.forEach((range, i) => {
        let hasErrors = range.value.errors.length > 0;
        let errors = <ul>{_.map(range.value.errors, (error, j) => {
          return <li key={j}>{error.message}</li>;
          })}</ul>;
        let row = [<td key={-1}>
            {range.key.toString()}
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
        range.value.nodes.forEach((node) => {
          let nodeRange = node.range;
          let replicaNodeIDs = nodeRange.state.state.desc.replicas.map((replica) => replica.node_id.toString());
          let index = nodeIDIndex[node.node_id];
          let cell = <td key={index}>
            {(this.state.showState) ? <div>State: {nodeRange.raft_state}</div> : ""}
            {(this.state.showReplicas) ? <div>
              <div>Replica On: {replicaNodeIDs.join(", ")}</div>
              <div>Next Replica ID: {nodeRange.state.state.desc.next_replica_id}</div>
            </div> : ""}
            {(this.state.showPending) ? <div>Pending Command Count: {(nodeRange.state.num_pending || 0).toString()}</div> : ""}
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
let rangeStatuses = (state: AdminUIState): cockroach.server.serverpb.RaftDebugResponse => state.cachedData.raft.data;

// Connect the RangesMain class with our redux store.
let rangesMainConnected = connect(
  (state: AdminUIState) => {
    return {
      rangeStatuses: rangeStatuses(state),
    };
  },
  {
    refreshRaft: refreshRaft,
  }
)(RangesMain);

export { rangesMainConnected as default };
