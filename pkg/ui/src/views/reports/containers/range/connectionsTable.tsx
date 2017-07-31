import _ from "lodash";
import React from "react";

import * as protos from "src/js/protos";

interface ConnectionsTableProps {
  rangeResponse: protos.cockroach.server.serverpb.RangeResponse$Properties;
}

export default function ConnectionsTable(props: ConnectionsTableProps) {
  const { rangeResponse } = props;
  if (_.isNil(rangeResponse)) {
    return null;
  }
  const ids = _.chain(_.keys(rangeResponse.responses_by_node_id))
    .map(id => parseInt(id, 10))
    .sortBy(id => id)
    .value();
  return (
    <div>
      <h2>Connections (via Node {rangeResponse.node_id})</h2>
      <table className="connections-table">
        <tbody>
          <tr className="connections-table__row connections-table__row--header">
            <th className="connections-table__cell connections-table__cell--header">Node</th>
            <th className="connections-table__cell connections-table__cell--header">Valid</th>
            <th className="connections-table__cell connections-table__cell--header">Replicas</th>
            <th className="connections-table__cell connections-table__cell--header">Error</th>
          </tr>
          {
            _.map(ids, id => {
              const resp = rangeResponse.responses_by_node_id[id];
              let rowClassName = "connections-table__row";
              if (!resp.response || !_.isEmpty(resp.error_message)) {
                rowClassName += " connections-table__row--warning";
              }
              return (
                <tr key={id} className={rowClassName}>
                  <td className="connections-table__cell">n{id}</td>
                  <td className="connections-table__cell">
                    {resp.response ? "ok" : "error"}
                  </td>
                  <td className="connections-table__cell">{resp.infos.length}</td>
                  <td className="connections-table__cell">{resp.error_message}</td>
                </tr>
              );
            })
          }
        </tbody>
      </table>
    </div>
  );
}
