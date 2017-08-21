import _ from "lodash";
import React from "react";

import * as protos from "src/js/protos";
import Print from "src/views/reports/containers/range/print";

interface AllocatorOutputProps {
  allocatorRangeResponse: protos.cockroach.server.serverpb.AllocatorRangeResponse;
}

export default class AllocatorOutput extends React.Component<AllocatorOutputProps, {}> {
  render() {
    const { allocatorRangeResponse } = this.props;
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
        <table className="log-table">
          <tbody>
            <tr className="log-table__row log-table__row--header">
              <th className="log-table__cell log-table__cell--header">Timestamp</th>
              <th className="log-table__cell log-table__cell--header">Message</th>
            </tr>
            {
              _.map(allocatorRangeResponse.dry_run.events, (event, key) => (
                <tr key={key} className="log-table__row">
                  <td className="log-table__cell">{Print.Timestamp(event.time)}</td>
                  <td className="log-table__cell">{event.message}</td>
                </tr>
              ))
            }
          </tbody>
        </table>
      </div>
    );
  }
}
