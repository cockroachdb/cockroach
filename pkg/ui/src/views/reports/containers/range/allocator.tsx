import _ from "lodash";
import React from "react";

import * as protos from "src/js/protos";

interface AllocatorOutputProps {
  info: protos.cockroach.server.serverpb.RangeInfo$Properties;
}

export default class AllocatorOutput extends React.Component<AllocatorOutputProps, {}> {
  renderLogInfoDescriptor(
    title: string, desc: string,
  ) {
    if (_.isEmpty(desc)) {
      return null;
    }
    return (
      <li>
        {title}: {desc}
      </li>
    );
  }

  renderLogInfo(info: protos.cockroach.server.serverpb.RangeResponse.RangeLog.PrettyInfo$Properties) {
    return (
      <ul className="log-entries-list">
        {this.renderLogInfoDescriptor("Updated Range Descriptor", info.updated_desc)}
        {this.renderLogInfoDescriptor("New Range Descriptor", info.new_desc)}
        {this.renderLogInfoDescriptor("Added Replica", info.added_replica)}
        {this.renderLogInfoDescriptor("Removed Replica", info.removed_replica)}
      </ul>
    );
  }

  render() {
    const { info } = this.props;
    if (_.isNil(info) || _.isEmpty(info) || _.isEmpty(info.simulated_allocator_output)) {
      return (
        <div>
          <h2>Simulated Allocator Output</h2>
          No simulated allocator output was returned.
        </div>
      );
    }

    return (
      <div>
        <h2>Simulated Allocator Output (from s{info.source_store_id})</h2>
        <table className="log-table">
          <tbody>
            {
              _.map(info.simulated_allocator_output, (line) => (
                <tr className="log-table__row">
                  <td className="log-table__cell">{line}</td>
                </tr>
              ))
            }
          </tbody>
        </table>
      </div>
    );
  }
}
