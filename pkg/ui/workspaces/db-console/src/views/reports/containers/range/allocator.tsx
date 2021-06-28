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

import * as protos from "src/js/protos";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { REMOTE_DEBUGGING_ERROR_TEXT } from "src/util/constants";
import Print from "src/views/reports/containers/range/print";
import { Loading } from "@cockroachlabs/cluster-ui";

interface AllocatorOutputProps {
  allocator: CachedDataReducerState<protos.cockroach.server.serverpb.AllocatorRangeResponse>;
}

export default class AllocatorOutput extends React.Component<
  AllocatorOutputProps,
  {}
> {
  renderContent = () => {
    const { allocator } = this.props;

    if (
      allocator &&
      (_.isEmpty(allocator.data) || _.isEmpty(allocator.data.dry_run))
    ) {
      return <div>No simulated allocator output was returned.</div>;
    }

    return (
      <table className="allocator-table">
        <tbody>
          <tr className="allocator-table__row allocator-table__row--header">
            <th className="allocator-table__cell allocator-table__cell--header">
              Timestamp
            </th>
            <th className="allocator-table__cell allocator-table__cell--header">
              Message
            </th>
          </tr>
          {_.map(allocator.data.dry_run.events, (event, key) => (
            <tr key={key} className="allocator-table__row">
              <td className="allocator-table__cell allocator-table__cell--date">
                {Print.Timestamp(event.time)}
              </td>
              <td className="allocator-table__cell">{event.message}</td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  };

  render() {
    const { allocator } = this.props;

    // TODO(couchand): This is a really myopic way to check for this particular
    // case, but making major changes to the CachedDataReducer or util.api seems
    // fraught at this point.  We should revisit this soon.
    if (
      allocator &&
      allocator.lastError &&
      allocator.lastError.message === "Forbidden"
    ) {
      return (
        <div>
          <h2 className="base-heading">Simulated Allocator Output</h2>
          {REMOTE_DEBUGGING_ERROR_TEXT}
        </div>
      );
    }

    let fromNodeID = "";
    if (allocator && !_.isEmpty(allocator.data)) {
      fromNodeID = ` (from n${allocator.data.node_id.toString()})`;
    }

    return (
      <div>
        <h2 className="base-heading">Simulated Allocator Output{fromNodeID}</h2>
        <Loading
          loading={!allocator || allocator.inFlight}
          error={allocator && allocator.lastError}
          render={this.renderContent}
        />
      </div>
    );
  }
}
