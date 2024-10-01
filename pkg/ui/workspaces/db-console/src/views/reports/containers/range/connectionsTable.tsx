// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Loading } from "@cockroachlabs/cluster-ui";
import classNames from "classnames";
import flow from "lodash/flow";
import isEmpty from "lodash/isEmpty";
import isNil from "lodash/isNil";
import keys from "lodash/keys";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import React from "react";

import * as protos from "src/js/protos";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";

interface ConnectionsTableProps {
  range: CachedDataReducerState<protos.cockroach.server.serverpb.RangeResponse>;
}

export default function ConnectionsTable(props: ConnectionsTableProps) {
  const { range } = props;
  let ids: number[];
  let viaNodeID = "";
  if (range && !range.inFlight && !isNil(range.data)) {
    ids = flow(
      keys,
      nodeIds => map(nodeIds, id => parseInt(id, 10)),
      nodeIds => sortBy(nodeIds, id => id),
    )(range.data.responses_by_node_id);
    viaNodeID = ` (via n${range.data.node_id.toString()})`;
  }

  return (
    <div>
      <h2 className="base-heading">Connections {viaNodeID}</h2>
      <Loading
        loading={!range || range.inFlight}
        page={"connections"}
        error={range && range.lastError}
        render={() => (
          <table className="connections-table">
            <tbody>
              <tr className="connections-table__row connections-table__row--header">
                <th className="connections-table__cell connections-table__cell--header">
                  Node
                </th>
                <th className="connections-table__cell connections-table__cell--header">
                  Valid
                </th>
                <th className="connections-table__cell connections-table__cell--header">
                  Replicas
                </th>
                <th className="connections-table__cell connections-table__cell--header">
                  Error
                </th>
              </tr>
              {map(ids, id => {
                const resp = range.data.responses_by_node_id[id];
                const rowClassName = classNames("connections-table__row", {
                  "connections-table__row--warning":
                    !resp.response || !isEmpty(resp.error_message),
                });
                return (
                  <tr key={id} className={rowClassName}>
                    <td className="connections-table__cell">n{id}</td>
                    <td className="connections-table__cell">
                      {resp.response ? "ok" : "error"}
                    </td>
                    <td className="connections-table__cell">
                      {resp.infos.length}
                    </td>
                    <td className="connections-table__cell">
                      {resp.error_message}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      />
    </div>
  );
}
