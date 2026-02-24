// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import filter from "lodash/filter";
import flatMap from "lodash/flatMap";
import flow from "lodash/flow";
import isNil from "lodash/isNil";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import uniq from "lodash/uniq";
import values from "lodash/values";
import moment from "moment-timezone";
import React, { useCallback, useEffect, useState } from "react";
import ReactPaginate from "react-paginate";
import { useDispatch, useSelector } from "react-redux";
import { Link } from "react-router-dom";

import * as protos from "src/js/protos";
import { refreshRaft } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { FixLong } from "src/util/fixLong";
import Print from "src/views/reports/containers/range/print";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

import RaftDebugResponse = cockroach.server.serverpb.RaftDebugResponse;

/******************************
 *   RAFT RANGES MAIN COMPONENT
 */

const RANGES_PER_PAGE = 100;

const selectRaftState = (
  state: AdminUIState,
): CachedDataReducerState<protos.cockroach.server.serverpb.RaftDebugResponse> =>
  state.cachedData.raft;

const RangesMain: React.FC<{ children?: React.ReactNode }> = ({ children }) => {
  const dispatch: AppDispatch = useDispatch();
  const raftState = useSelector(selectRaftState);

  const [showState, setShowState] = useState(true);
  const [showReplicas, setShowReplicas] = useState(true);
  const [showPending, setShowPending] = useState(true);
  const [showOnlyErrors, setShowOnlyErrors] = useState(false);
  const [offset, setOffset] = useState(0);

  useEffect(() => {
    // Refresh nodes status query when mounting.
    dispatch(refreshRaft());
  }, [dispatch]);

  useEffect(() => {
    // Refresh ranges when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    if (!raftState.valid) {
      dispatch(refreshRaft());
    }
  }, [dispatch, raftState.valid]);

  const handlePageClick = useCallback((data: any) => {
    const selected = data.selected;
    const newOffset = Math.ceil(selected * RANGES_PER_PAGE);
    setOffset(newOffset);
    window.scroll(0, 0);
  }, []);

  const renderPagination = (pageCount: number): React.ReactNode => {
    return (
      <ReactPaginate
        previousLabel={"previous"}
        nextLabel={"next"}
        breakLabel={"..."}
        pageCount={pageCount}
        marginPagesDisplayed={2}
        pageRangeDisplayed={5}
        onPageChange={handlePageClick}
        containerClassName={"pagination"}
        activeClassName={"active"}
      />
    );
  };

  // renderFilterSettings renders the filter settings box.
  const renderFilterSettings = (): React.ReactNode => {
    return (
      <div className="section raft-filters">
        <b>Filters</b>
        <label>
          <input
            type="checkbox"
            checked={showState}
            onChange={() => setShowState(prev => !prev)}
          />
          State
        </label>
        <label>
          <input
            type="checkbox"
            checked={showReplicas}
            onChange={() => setShowReplicas(prev => !prev)}
          />
          Replicas
        </label>
        <label>
          <input
            type="checkbox"
            checked={showPending}
            onChange={() => setShowPending(prev => !prev)}
          />
          Pending
        </label>
        <label>
          <input
            type="checkbox"
            checked={showOnlyErrors}
            onChange={() => setShowOnlyErrors(prev => !prev)}
          />
          Only Error Ranges
        </label>
      </div>
    );
  };

  const renderErrors = (errors: string[]) => {
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
  };

  const statuses = raftState.data;
  let content: React.ReactNode = null;
  let errors: string[] = [];

  if (raftState.lastError) {
    errors.push(raftState.lastError.message);
  }

  if (!raftState.data) {
    content = <div className="section">Loading...</div>;
  } else if (statuses) {
    errors = errors.concat(statuses.errors.map(err => err.message));

    // Build list of all nodes for static ordering.
    const nodeIDs = flow(
      (ranges: RaftDebugResponse["ranges"]) => flatMap(ranges, r => r.nodes),
      nodes => map(nodes, node => node.node_id),
      nodeIds => uniq(nodeIds),
      nodeIds => sortBy(nodeIds),
    )(statuses.ranges);

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
    const justRanges = values(statuses.ranges);
    const filteredRanges = filter(justRanges, range => {
      return !showOnlyErrors || range.errors.length > 0;
    });
    let currentOffset = offset;
    if (offset > filteredRanges.length) {
      currentOffset = 0;
    }
    const ranges = filteredRanges.slice(
      currentOffset,
      currentOffset + RANGES_PER_PAGE,
    );
    const rows: React.ReactNode[][] = [];
    map(ranges, (range, i) => {
      const hasErrors = range.errors.length > 0;
      const rangeErrors = (
        <ul>
          {map(range.errors, (error, j) => {
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
      range.nodes.forEach(node => {
        const nodeRange = node.range;
        const replicaLocations =
          nodeRange.state.state.desc.internal_replicas.map(
            replica =>
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

        const displayTimestamp = (
          timestamp: protos.cockroach.util.hlc.ITimestamp,
          now: moment.Moment,
        ): string => {
          if (isNil(timestamp) || isNil(timestamp.wall_time)) {
            return "nil";
          }

          if (FixLong(timestamp.wall_time).isZero()) {
            return "empty";
          }

          const humanized = Print.Timestamp(timestamp);
          const delta = Print.TimestampDeltaFromNow(timestamp, now);
          return humanized.concat(", ", delta);
        };

        const now = moment();
        const index = nodeIDIndex[node.node_id];
        const raftNodeState = nodeRange.raft_state;
        const cell = (
          <td key={index}>
            {showState ? (
              <div>
                State: {raftNodeState.state}&nbsp; ReplicaID=
                {display(raftNodeState.replica_id)}&nbsp; Term=
                {display(raftNodeState.hard_state.term)}&nbsp; Lead=
                {display(raftNodeState.lead)}&nbsp; LeadSupportUntil=
                {displayTimestamp(raftNodeState.lead_support_until, now)}&nbsp;
              </div>
            ) : (
              ""
            )}
            {showReplicas ? (
              <div>
                <div>Replica On: {replicaLocations.join(", ")}</div>
                <div>
                  Next Replica ID: {nodeRange.state.state.desc.next_replica_id}
                </div>
              </div>
            ) : (
              ""
            )}
            {showPending ? (
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
          {renderFilterSettings()}
          <table>
            <thead>
              <tr>{columns}</tr>
            </thead>
            <tbody>
              {values(rows).map((row: React.ReactNode[], i: number) => {
                return <tr key={i}>{row}</tr>;
              })}
            </tbody>
          </table>
          <div className="section">
            {renderPagination(
              Math.ceil(filteredRanges.length / RANGES_PER_PAGE),
            )}
          </div>
        </div>
      );
    }
  }
  return (
    <div className="section table">
      {children}
      <div className="stats-table">
        {renderErrors(errors)}
        {content}
      </div>
    </div>
  );
};

export default RangesMain;
