import classNames from "classnames";
import _ from "lodash";
import React from "react";

import * as protos from "src/js/protos";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import Loading from "src/views/shared/components/loading";

import spinner from "assets/spinner.gif";

interface ConnectionsTableProps {
  range: CachedDataReducerState<protos.cockroach.server.serverpb.RangeResponse>;
}

export default function ConnectionsTable(props: ConnectionsTableProps) {
  const { range } = props;
  let ids: number[];
  let viaNodeID = "";
  if (range && !range.inFlight && !_.isNil(range.data)) {
    ids = _.chain(_.keys(range.data.responses_by_node_id))
      .map(id => parseInt(id, 10))
      .sortBy(id => id)
      .value();
    viaNodeID = ` (via n${range.data.node_id.toString()})`;
  }

  return (
    <div>
      <h2>Connections {viaNodeID}</h2>
      <Loading
        loading={!range || range.inFlight}
        className="loading-image loading-image__spinner-left"
        image={spinner}
      >
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
                const resp = range.data.responses_by_node_id[id];
                const rowClassName = classNames(
                  "connections-table__row",
                  { "connections-table__row--warning": !resp.response || !_.isEmpty(resp.error_message) },
                );
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
      </Loading>
    </div>
  );
}
