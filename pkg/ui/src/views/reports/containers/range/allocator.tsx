import _ from "lodash";
import React from "react";

import * as protos from "src/js/protos";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import Print from "src/views/reports/containers/range/print";
import Loading from "src/views/shared/components/loading";

import spinner from "assets/spinner.gif";

interface AllocatorOutputProps {
  allocatorRangeResponse: CachedDataReducerState<protos.cockroach.server.serverpb.AllocatorRangeResponse>;
}

export default class AllocatorOutput extends React.Component<AllocatorOutputProps, {}> {
  render() {
    const { allocatorRangeResponse } = this.props;

    if (allocatorRangeResponse && !_.isNil(allocatorRangeResponse.lastError)) {
      return (
        <div>
          <h2>Simulated Allocator Output</h2>
          {allocatorRangeResponse.lastError.toString()}
        </div>
      );
    }

    if (allocatorRangeResponse && (_.isEmpty(allocatorRangeResponse.data) || _.isEmpty(allocatorRangeResponse.data.dry_run))) {
      return (
        <div>
          <h2>Simulated Allocator Output</h2>
          No simulated allocator output was returned.
        </div>
      );
    }

    let fromNodeID = "";
    if (allocatorRangeResponse && !_.isEmpty(allocatorRangeResponse.data)) {
      fromNodeID = ` (from n${allocatorRangeResponse.data.node_id.toString()})`;
    }

    return (
      <div>
        <h2>Simulated Allocator Output {fromNodeID}</h2>
        <Loading
          loading={allocatorRangeResponse && allocatorRangeResponse.inFlight}
          className="loading-image loading-image__spinner-left"
          image={spinner}
        >
          <table className="allocator-table">
            <tbody>
              <tr className="allocator-table__row allocator-table__row--header">
                <th className="allocator-table__cell allocator-table__cell--header">Timestamp</th>
                <th className="allocator-table__cell allocator-table__cell--header">Message</th>
              </tr>
              {
                _.map(allocatorRangeResponse.data.dry_run.events, (event, key) => (
                  <tr key={key} className="allocator-table__row">
                    <td className="allocator-table__cell allocator-table__cell--date">{Print.Timestamp(event.time)}</td>
                    <td className="allocator-table__cell">{event.message}</td>
                  </tr>
                ))
              }
            </tbody>
          </table>
        </Loading>
      </div>
    );
  }
}
