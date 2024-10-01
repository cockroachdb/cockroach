// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames";
import flow from "lodash/flow";
import isEmpty from "lodash/isEmpty";
import isNil from "lodash/isNil";
import keys from "lodash/keys";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import React from "react";
import { Link } from "react-router-dom";

import * as protos from "src/js/protos";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";

interface ConnectionTableColumn {
  title: string;
  extract: (
    p: protos.cockroach.server.serverpb.ProblemRangesResponse.INodeProblems,
    id?: number,
  ) => React.ReactNode;
}

interface ConnectionsTableProps {
  problemRanges: CachedDataReducerState<protos.cockroach.server.serverpb.ProblemRangesResponse>;
}

const connectionTableColumns: ConnectionTableColumn[] = [
  {
    title: "Node",
    extract: (_problem, id) => (
      <Link className="debug-link" to={`/reports/problemranges/${id}`}>
        n{id}
      </Link>
    ),
  },
  {
    title: "Unavailable",
    extract: problem => problem.unavailable_range_ids.length,
  },
  {
    title: "No Raft Leader",
    extract: problem => problem.no_raft_leader_range_ids.length,
  },
  {
    title: "Expired Lease",
    extract: problem => problem.no_lease_range_ids.length,
  },
  {
    title: "Raft Leader but not Lease Holder",
    extract: problem => problem.raft_leader_not_lease_holder_range_ids.length,
  },
  {
    title: "Underreplicated (or slow)",
    extract: problem => problem.underreplicated_range_ids.length,
  },
  {
    title: "Overreplicated",
    extract: problem => problem.overreplicated_range_ids.length,
  },
  {
    title: "Quiescent equals ticking",
    extract: problem => problem.quiescent_equals_ticking_range_ids.length,
  },
  {
    title: "Raft log too large",
    extract: problem => problem.raft_log_too_large_range_ids.length,
  },
  {
    title: "Circuit breaker error",
    extract: problem => problem.circuit_breaker_error_range_ids.length,
  },
  {
    title: "Paused Replicas",
    extract: problem => problem.paused_replica_ids.length,
  },
  {
    title: "Range Too Large",
    extract: problem => problem.too_large_range_ids.length,
  },
  {
    title: "Total",
    extract: problem => {
      return (
        problem.unavailable_range_ids.length +
        problem.no_raft_leader_range_ids.length +
        problem.no_lease_range_ids.length +
        problem.raft_leader_not_lease_holder_range_ids.length +
        problem.underreplicated_range_ids.length +
        problem.overreplicated_range_ids.length +
        problem.quiescent_equals_ticking_range_ids.length +
        problem.raft_log_too_large_range_ids.length +
        problem.circuit_breaker_error_range_ids.length +
        problem.paused_replica_ids.length +
        problem.too_large_range_ids.length
      );
    },
  },
  { title: "Error", extract: problem => problem.error_message },
];

export default function ConnectionsTable(props: ConnectionsTableProps) {
  const { problemRanges } = props;
  // lastError is already handled by ProblemRanges component.
  if (
    isNil(problemRanges) ||
    isNil(problemRanges.data) ||
    !isNil(problemRanges.lastError)
  ) {
    return null;
  }
  const { data } = problemRanges;
  const ids = flow(
    keys,
    nodeIds => map(nodeIds, id => parseInt(id, 10)),
    nodeIds => sortBy(nodeIds, id => id),
  )(data.problems_by_node_id);

  return (
    <div>
      <h2 className="base-heading">Connections (via Node {data.node_id})</h2>
      <table className="connections-table">
        <tbody>
          <tr className="connections-table__row connections-table__row--header">
            {map(connectionTableColumns, (col, key) => (
              <th
                key={key}
                className="connections-table__cell connections-table__cell--header"
              >
                {col.title}
              </th>
            ))}
          </tr>
          {map(ids, id => {
            const rowProblems = data.problems_by_node_id[id];
            const rowClassName = classNames({
              "connections-table__row": true,
              "connections-table__row--warning": !isEmpty(
                rowProblems.error_message,
              ),
            });
            return (
              <tr key={id} className={rowClassName}>
                {map(connectionTableColumns, (col, key) => (
                  <td key={key} className="connections-table__cell">
                    {col.extract(rowProblems, id)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
