// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import React from "react";

import * as protos from "src/js/protos";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { REMOTE_DEBUGGING_ERROR_TEXT } from "src/util/constants";
import Print from "src/views/reports/containers/range/print";
import Loading from "src/views/shared/components/loading";

import spinner from "assets/spinner.gif";

interface AllocatorOutputProps {
  allocator: CachedDataReducerState<protos.cockroach.server.serverpb.AllocatorRangeResponse>;
}

export default class AllocatorOutput extends React.Component<AllocatorOutputProps, {}> {
  render() {
    const { allocator } = this.props;

    // TODO(couchand): This is a really myopic way to check for this particular
    // case, but making major changes to the CachedDataReducer or util.api seems
    // fraught at this point.  We should revisit this soon.
    if (allocator.lastError && allocator.lastError.message === "Forbidden") {
      return (
        <div>
          <h2>Simulated Allocator Output</h2>
          { REMOTE_DEBUGGING_ERROR_TEXT }
        </div>
      );
    }

    if (allocator && !_.isNil(allocator.lastError)) {
      return (
        <div>
          <h2>Simulated Allocator Output</h2>
          {allocator.lastError.toString()}
        </div>
      );
    }

    if (allocator && (_.isEmpty(allocator.data) || _.isEmpty(allocator.data.dry_run))) {
      return (
        <div>
          <h2>Simulated Allocator Output</h2>
          No simulated allocator output was returned.
        </div>
      );
    }

    let fromNodeID = "";
    if (allocator && !_.isEmpty(allocator.data)) {
      fromNodeID = ` (from n${allocator.data.node_id.toString()})`;
    }

    return (
      <div>
        <h2>Simulated Allocator Output{fromNodeID}</h2>
        <Loading
          loading={!allocator || allocator.inFlight}
          className="loading-image loading-image__spinner-left"
          image={spinner}
          render={() => (
            <table className="allocator-table">
              <tbody>
                <tr className="allocator-table__row allocator-table__row--header">
                  <th className="allocator-table__cell allocator-table__cell--header">Timestamp</th>
                  <th className="allocator-table__cell allocator-table__cell--header">Message</th>
                </tr>
                {
                  _.map(allocator.data.dry_run.events, (event, key) => (
                    <tr key={key} className="allocator-table__row">
                      <td className="allocator-table__cell allocator-table__cell--date">{Print.Timestamp(event.time)}</td>
                      <td className="allocator-table__cell">{event.message}</td>
                    </tr>
                  ))
                }
              </tbody>
            </table>
          )}
        />
      </div>
    );
  }
}
