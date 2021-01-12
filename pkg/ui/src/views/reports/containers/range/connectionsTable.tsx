// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classNames from "classnames";
import _ from "lodash";
import React from "react";

import * as protos from "src/js/protos";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { Loading } from "@cockroachlabs/cluster-ui";

interface ConnectionsTableProps {
  range: CachedDataReducerState<protos.cockroach.server.serverpb.RangeResponse>;
}

export default function ConnectionsTable(props: ConnectionsTableProps) {
  const { range } = props;
  let ids: number[];
  let viaNodeID = "";
  if (range && !range.inFlight && !_.isNil(range.data)) {
    ids = _.chain(_.keys(range.data.responses_by_node_id))
      .map((id) => parseInt(id, 10))
      .sortBy((id) => id)
      .value();
    viaNodeID = ` (via n${range.data.node_id.toString()})`;
  }

  return (
    <div>
      <h2 className="base-heading">Connections {viaNodeID}</h2>
      <Loading
        loading={!range || range.inFlight}
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
              {_.map(ids, (id) => {
                const resp = range.data.responses_by_node_id[id];
                const rowClassName = classNames("connections-table__row", {
                  "connections-table__row--warning":
                    !resp.response || !_.isEmpty(resp.error_message),
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
