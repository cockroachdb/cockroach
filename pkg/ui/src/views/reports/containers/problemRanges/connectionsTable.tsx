import _ from "lodash";
import React from "react";
import { Link } from "react-router";

import * as protos from "src/js/protos";

interface ConnectionsTableProps {
  problemRanges: protos.cockroach.server.serverpb.ProblemRangesResponse;
}

function ConnectionTableRow(props: {
  nodeID: number,
  problems:  protos.cockroach.server.serverpb.ProblemRangesResponse.NodeProblems$Properties,
}) {
  const { nodeID, problems } = props;
  let rowClassName = "connections-table__row";
  if (!_.isEmpty(props.problems.error_message)) {
    rowClassName += " connections-table__row--warning";
  }
  return (
    <tr key={nodeID} className={rowClassName}>
      <td className="connections-table__cell">
        <Link className="debug-link" to={`/reports/problemranges/${nodeID}`}>n{nodeID}</Link>
      </td>
      <td className="connections-table__cell">{problems.unavailable_range_ids.length}</td>
      <td className="connections-table__cell">{problems.no_raft_leader_range_ids.length}</td>
      <td className="connections-table__cell">{problems.no_lease_range_ids.length}</td>
      <td className="connections-table__cell">{problems.raft_leader_not_lease_holder_range_ids.length}</td>
      <td className="connections-table__cell">{problems.underreplicated_range_ids.length}</td>
      <td className="connections-table__cell">
      {
        problems.unavailable_range_ids.length +
        problems.no_raft_leader_range_ids.length +
        problems.no_lease_range_ids.length +
        problems.raft_leader_not_lease_holder_range_ids.length +
        problems.underreplicated_range_ids.length
      }
      </td>
      <td className="connections-table__cell">{problems.error_message}</td>
    </tr>
  );
}

export default function ConnectionsTable(props: ConnectionsTableProps) {
  const { problemRanges } = props;
  if (_.isNil(problemRanges)) {
    return null;
  }
  const ids = _.chain(_.keys(problemRanges.problems_by_node_id))
    .map(id => parseInt(id, 10))
    .sortBy(id => id)
    .value();
  return (
    <div>
      <h2>Connections (via Node {problemRanges.node_id})</h2>
      <table className="connections-table">
        <tbody>
          <tr className="connections-table__row connections-table__row--header">
            <th className="connections-table__cell connections-table__cell--header">Node</th>
            <th className="connections-table__cell connections-table__cell--header">Unavailable</th>
            <th className="connections-table__cell connections-table__cell--header">No Raft Leader</th>
            <th className="connections-table__cell connections-table__cell--header">No Lease</th>
            <th className="connections-table__cell connections-table__cell--header">Raft Leader but not Lease Holder</th>
            <th className="connections-table__cell connections-table__cell--header">Underreplicated (or slow)</th>
            <th className="connections-table__cell connections-table__cell--header">Total</th>
            <th className="connections-table__cell connections-table__cell--header">Error</th>
          </tr>
          {
            _.map(ids, id => {
              return (
                <ConnectionTableRow
                  key={id}
                  nodeID={id}
                  problems={problemRanges.problems_by_node_id[id]}
                />
              );
            })
          }
        </tbody>
      </table>
    </div>
  );
}
