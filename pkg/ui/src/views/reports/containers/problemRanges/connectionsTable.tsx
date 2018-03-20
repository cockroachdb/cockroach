import _ from "lodash";
import classNames from "classnames";
import React from "react";
import { Link } from "react-router";

import * as protos from "src/js/protos";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";

interface ConnectionTableColumn {
  title: string;
  extract: (
    p: protos.cockroach.server.serverpb.ProblemRangesResponse.NodeProblems$Properties,
    id?: number,
  ) => React.ReactNode;
}

interface ConnectionsTableProps {
  problemRanges: CachedDataReducerState<protos.cockroach.server.serverpb.ProblemRangesResponse>;
}

const connectionTableColumns: ConnectionTableColumn[] = [
  {
    title: "Node",
    extract: (_, id) => (
      <Link className="debug-link" to={`/reports/problemranges/${id}`}>
        n{id}
      </Link>
    ),
  },
  { title: "Unavailable", extract: (problem) => problem.unavailable_range_ids.length },
  { title: "No Raft Leader", extract: (problem) => problem.no_raft_leader_range_ids.length },
  { title: "Invalid Lease", extract: (problem) => problem.no_lease_range_ids.length },
  {
    title: "Raft Leader but not Lease Holder",
    extract: (problem) => problem.raft_leader_not_lease_holder_range_ids.length,
  },
  {
    title: "Underreplicated (or slow)",
    extract: (problem) => problem.underreplicated_range_ids.length,
  },
  {
    title: "Total",
    extract: (problem) => {
      return problem.unavailable_range_ids.length +
        problem.no_raft_leader_range_ids.length +
        problem.no_lease_range_ids.length +
        problem.raft_leader_not_lease_holder_range_ids.length +
        problem.underreplicated_range_ids.length;
    },
  },
  { title: "Error", extract: (problem) => problem.error_message },
];

export default function ConnectionsTable(props: ConnectionsTableProps) {
  const { problemRanges } = props;
  // lastError is already handled by ProblemRanges component.
  if (_.isNil(problemRanges) ||
    _.isNil(problemRanges.data) ||
    !_.isNil(problemRanges.lastError)) {
    return null;
  }
  const { data } = problemRanges;
  const ids = _.chain(_.keys(data.problems_by_node_id))
    .map(id => parseInt(id, 10))
    .sortBy(id => id)
    .value();
  return (
    <div>
      <h2>Connections (via Node {data.node_id})</h2>
      <table className="connections-table">
        <tbody>
          <tr className="connections-table__row connections-table__row--header">
            {
              _.map(connectionTableColumns, (col, key) => (
                <th key={key} className="connections-table__cell connections-table__cell--header">
                  {col.title}
                </th>
              ))
            }
          </tr>
          {
            _.map(ids, id => {
              const rowProblems = data.problems_by_node_id[id];
              const rowClassName = classNames({
                "connections-table__row": true,
                "connections-table__row--warning": !_.isEmpty(rowProblems.error_message),
              });
              return (
                <tr key={id} className={rowClassName}>
                  {
                    _.map(connectionTableColumns, (col, key) => (
                      <td key={key} className="connections-table__cell">
                        {col.extract(rowProblems, id)}
                      </td>
                    ))
                  }
                </tr>
              );
            })
          }
        </tbody>
      </table>
    </div>
  );
}
