// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import ReactPaginate from "react-paginate";
import { connect } from "react-redux";
import { Link, withRouter } from "react-router-dom";
import * as protos from "src/js/protos";
import { refreshRaft } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

/******************************
 *   RAFT RANGES MAIN COMPONENT
 */

const RANGES_PER_PAGE = 100;

/**
 * RangesMainData are the data properties which should be passed to the RangesMain
 * container.
 */
interface RangesMainData {
  state: CachedDataReducerState<protos.cockroach.server.serverpb.RaftDebugResponse>;
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
export class RangesMain extends React.Component<
  RangesMainProps,
  RangesMainState
> {
  state: RangesMainState = {
    showState: true,
    showReplicas: true,
    showPending: true,
    showOnlyErrors: false,
    offset: 0,
  };

  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshRaft();
  }

  componentDidUpdate() {
    // Refresh ranges when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    if (!this.props.state.valid) {
      this.props.refreshRaft();
    }
  }

  renderPagination(pageCount: number): React.ReactNode {
    return (
      <ReactPaginate
        previousLabel={"previous"}
        nextLabel={"next"}
        breakLabel={"..."}
        pageCount={pageCount}
        marginPagesDisplayed={2}
        pageRangeDisplayed={5}
        onPageChange={this.handlePageClick.bind(this)}
        containerClassName={"pagination"}
        activeClassName={"active"}
      />
    );
  }

  handlePageClick(data: any) {
    const selected = data.selected;
    const offset = Math.ceil(selected * RANGES_PER_PAGE);
    this.setState({ offset });
    window.scroll(0, 0);
  }

  // renderFilterSettings renders the filter settings box.
  renderFilterSettings(): React.ReactNode {
    return (
      <div className="section raft-filters">
        <b>Filters</b>
        <label>
          <input
            type="checkbox"
            checked={this.state.showState}
            onChange={() => this.setState({ showState: !this.state.showState })}
          />
          State
        </label>
        <label>
          <input
            type="checkbox"
            checked={this.state.showReplicas}
            onChange={() =>
              this.setState({ showReplicas: !this.state.showReplicas })
            }
          />
          Replicas
        </label>
        <label>
          <input
            type="checkbox"
            checked={this.state.showPending}
            onChange={() =>
              this.setState({ showPending: !this.state.showPending })
            }
          />
          Pending
        </label>
        <label>
          <input
            type="checkbox"
            checked={this.state.showOnlyErrors}
            onChange={() =>
              this.setState({ showOnlyErrors: !this.state.showOnlyErrors })
            }
          />
          Only Error Ranges
        </label>
      </div>
    );
  }

  render() {
    const statuses = this.props.state.data;
    let content: React.ReactNode = null;
    let errors: string[] = [];

    if (this.props.state.lastError) {
      errors.push(this.props.state.lastError.message);
    }

    if (!this.props.state.data) {
      content = <div className="section">Loading...</div>;
    } else if (statuses) {
      errors = errors.concat(statuses.errors.map((err) => err.message));

      // Build list of all nodes for static ordering.
      const nodeIDs = _(statuses.ranges)
        .flatMap((range: protos.cockroach.server.serverpb.IRaftRangeStatus) => {
          return range.nodes;
        })
        .map((node: protos.cockroach.server.serverpb.IRaftRangeNode) => {
          return node.node_id;
        })
        .uniq()
        .sort()
        .value();

      const nodeIDIndex: { [nodeID: number]: number } = {};
      const columns = [<th key={-1}>Range</th>];
      nodeIDs.forEach((id, i) => {
        nodeIDIndex[id] = i + 1;
        columns.push(
          <th key={i}>
            <Link className="debug-link" to={"/nodes/" + id}>
              Node {id}
            </Link>
          </th>,
        );
      });

      // Filter ranges and paginate
      const justRanges = _.values(statuses.ranges);
      const filteredRanges = _.filter(justRanges, (range) => {
        return !this.state.showOnlyErrors || range.errors.length > 0;
      });
      let offset = this.state.offset;
      if (this.state.offset > filteredRanges.length) {
        offset = 0;
      }
      const ranges = filteredRanges.slice(offset, offset + RANGES_PER_PAGE);
      const rows: React.ReactNode[][] = [];
      _.map(ranges, (range, i) => {
        const hasErrors = range.errors.length > 0;
        const rangeErrors = (
          <ul>
            {_.map(range.errors, (error, j) => {
              return <li key={j}>{error.message}</li>;
            })}
          </ul>
        );
        const row = [
          <td key="row{i}">
            <Link
              className="debug-link"
              to={`/reports/range/${range.range_id.toString()}`}
            >
              r{range.range_id.toString()}
            </Link>
            {hasErrors ? (
              <span style={{ position: "relative" }}>
                <ToolTipWrapper text={rangeErrors}>
                  <div className="viz-info-icon">
                    <div className="icon-warning" />
                  </div>
                </ToolTipWrapper>
              </span>
            ) : (
              ""
            )}
          </td>,
        ];
        rows[i] = row;

        // Render each replica into a cell
        range.nodes.forEach((node) => {
          const nodeRange = node.range;
          const replicaLocations = nodeRange.state.state.desc.internal_replicas.map(
            (replica) =>
              "(Node " +
              replica.node_id.toString() +
              " Store " +
              replica.store_id.toString() +
              " ReplicaID " +
              replica.replica_id.toString() +
              ")",
          );
          const display = (l?: Long): string => {
            if (l) {
              return l.toString();
            }
            return "N/A";
          };
          const index = nodeIDIndex[node.node_id];
          const raftState = nodeRange.raft_state;
          const cell = (
            <td key={index}>
              {this.state.showState ? (
                <div>
                  State: {raftState.state}&nbsp; ReplicaID=
                  {display(raftState.replica_id)}&nbsp; Term=
                  {display(raftState.hard_state.term)}&nbsp; Lead=
                  {display(raftState.lead)}&nbsp;
                </div>
              ) : (
                ""
              )}
              {this.state.showReplicas ? (
                <div>
                  <div>Replica On: {replicaLocations.join(", ")}</div>
                  <div>
                    Next Replica ID:{" "}
                    {nodeRange.state.state.desc.next_replica_id}
                  </div>
                </div>
              ) : (
                ""
              )}
              {this.state.showPending ? (
                <div>
                  Pending Command Count:{" "}
                  {(nodeRange.state.num_pending || 0).toString()}
                </div>
              ) : (
                ""
              )}
            </td>
          );
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
      if (columns.length > 1) {
        content = (
          <div>
            {this.renderFilterSettings()}
            <table>
              <thead>
                <tr>{columns}</tr>
              </thead>
              <tbody>
                {_.values(rows).map((row: React.ReactNode[], i: number) => {
                  return <tr key={i}>{row}</tr>;
                })}
              </tbody>
            </table>
            <div className="section">
              {this.renderPagination(
                Math.ceil(filteredRanges.length / RANGES_PER_PAGE),
              )}
            </div>
          </div>
        );
      }
    }
    return (
      <div className="section table">
        {this.props.children}
        <div className="stats-table">
          {this.renderErrors(errors)}
          {content}
        </div>
      </div>
    );
  }

  renderErrors(errors: string[]) {
    if (!errors || errors.length === 0) {
      return;
    }
    return (
      <div className="section">
        {errors.map((err: string, i: number) => {
          return <div key={i}>Error: {err}</div>;
        })}
      </div>
    );
  }
}

/******************************
 *         SELECTORS
 */

// Base selectors to extract data from redux state.
const selectRaftState = (
  state: AdminUIState,
): CachedDataReducerState<protos.cockroach.server.serverpb.RaftDebugResponse> =>
  state.cachedData.raft;

const mapStateToProps = (state: AdminUIState) => ({
  // RootState contains declaration for whole state
  state: selectRaftState(state),
});

const mapDispatchToProps = {
  refreshRaft,
};

// Connect the RangesMain class with our redux store.
const rangesMainConnected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(RangesMain),
);

export { rangesMainConnected as default };
