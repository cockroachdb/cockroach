import _ from "lodash";
import React from "react";

import * as protos from "src/js/protos";
import Print from "src/views/reports/containers/range/print";

interface AllocatorOutputProps {
  allocatorRangeResponse: protos.cockroach.server.serverpb.AllocatorRangeResponse;
  lastError: Error;
}

export default class AllocatorOutput extends React.Component<AllocatorOutputProps, {}> {
  render() {
    const { allocatorRangeResponse, lastError } = this.props;

    if (!_.isNil(lastError)) {
      return (
        <div>
          <h2>Simulated Allocator Output</h2>
          {lastError.toString()}
        </div>
      );
    }

    if (_.isEmpty(allocatorRangeResponse) || _.isEmpty(allocatorRangeResponse.dry_run)) {
      return (
        <div>
          <h2>Simulated Allocator Output</h2>
          No simulated allocator output was returned.
        </div>
      );
    }

    return (
      <div>
        <h2>Simulated Allocator Output (from n{allocatorRangeResponse.node_id.toString()})</h2>
        <table className="allocator-table">
          <tbody>
            <tr className="allocator-table__row allocator-table__row--header">
              <th className="allocator-table__cell allocator-table__cell--header">Timestamp</th>
              <th className="allocator-table__cell allocator-table__cell--header">Message</th>
            </tr>
            {
              _.map(allocatorRangeResponse.dry_run.events, (event, key) => (
                <tr key={key} className="allocator-table__row">
                  <td className="allocator-table__cell allocator-table__cell--date">{Print.Timestamp(event.time)}</td>
                  <td className="allocator-table__cell">{event.message}</td>
                </tr>
              ))
            }
          </tbody>
        </table>
      </div>
    );
  }
}
