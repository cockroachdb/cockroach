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
import { FixLong } from "src/util/fixLong";
import Print from "src/views/reports/containers/range/print";
import { Loading } from "@cockroachlabs/cluster-ui";
import { TimestampToMoment } from "src/util/convert";

interface LogTableProps {
  rangeID: Long;
  log: CachedDataReducerState<protos.cockroach.server.serverpb.RangeLogResponse>;
}

function printLogEventType(
  eventType: protos.cockroach.kv.kvserver.storagepb.RangeLogEventType,
) {
  switch (eventType) {
    case protos.cockroach.kv.kvserver.storagepb.RangeLogEventType.add_voter:
      return "Add Voter";
    case protos.cockroach.kv.kvserver.storagepb.RangeLogEventType.remove_voter:
      return "Remove Voter";
    case protos.cockroach.kv.kvserver.storagepb.RangeLogEventType.add_non_voter:
      return "Add Non-Voter";
    case protos.cockroach.kv.kvserver.storagepb.RangeLogEventType
      .remove_non_voter:
      return "Remove Non-Voter";
    case protos.cockroach.kv.kvserver.storagepb.RangeLogEventType.split:
      return "Split";
    case protos.cockroach.kv.kvserver.storagepb.RangeLogEventType.merge:
      return "Merge";
    default:
      return "Unknown";
  }
}

export default class LogTable extends React.Component<LogTableProps, {}> {
  // If there is no otherRangeID, it comes back as the number 0.
  renderRangeID(otherRangeID: Long | number) {
    const fixedOtherRangeID = FixLong(otherRangeID);
    const fixedCurrentRangeID = FixLong(this.props.rangeID);
    if (fixedOtherRangeID.eq(0)) {
      return null;
    }

    if (fixedCurrentRangeID.eq(fixedOtherRangeID)) {
      return `r${fixedOtherRangeID.toString()}`;
    }

    return (
      <a href={`#/reports/range/${fixedOtherRangeID.toString()}`}>
        r{fixedOtherRangeID.toString()}
      </a>
    );
  }

  renderLogInfoDescriptor(title: string, desc: string) {
    if (_.isEmpty(desc)) {
      return null;
    }
    return (
      <li>
        {title}: {desc}
      </li>
    );
  }

  renderLogInfo(
    info: protos.cockroach.server.serverpb.RangeLogResponse.IPrettyInfo,
  ) {
    return (
      <ul className="log-entries-list">
        {this.renderLogInfoDescriptor(
          "Updated Range Descriptor",
          info.updated_desc,
        )}
        {this.renderLogInfoDescriptor("New Range Descriptor", info.new_desc)}
        {this.renderLogInfoDescriptor("Added Replica", info.added_replica)}
        {this.renderLogInfoDescriptor("Removed Replica", info.removed_replica)}
        {this.renderLogInfoDescriptor("Reason", info.reason)}
        {this.renderLogInfoDescriptor("Details", info.details)}
      </ul>
    );
  }

  renderContent = () => {
    const { log } = this.props;

    // Sort by descending timestamp.
    const events = _.orderBy(
      log && log.data && log.data.events,
      (event) => TimestampToMoment(event.event.timestamp).valueOf(),
      "desc",
    );

    return (
      <table className="log-table">
        <tbody>
          <tr className="log-table__row log-table__row--header">
            <th className="log-table__cell log-table__cell--header">
              Timestamp
            </th>
            <th className="log-table__cell log-table__cell--header">Store</th>
            <th className="log-table__cell log-table__cell--header">
              Event Type
            </th>
            <th className="log-table__cell log-table__cell--header">Range</th>
            <th className="log-table__cell log-table__cell--header">
              Other Range
            </th>
            <th className="log-table__cell log-table__cell--header">Info</th>
          </tr>
          {_.map(events, (event, key) => (
            <tr key={key} className="log-table__row">
              <td className="log-table__cell log-table__cell--date">
                {Print.Timestamp(event.event.timestamp)}
              </td>
              <td className="log-table__cell">s{event.event.store_id}</td>
              <td className="log-table__cell">
                {printLogEventType(event.event.event_type)}
              </td>
              <td className="log-table__cell">
                {this.renderRangeID(event.event.range_id)}
              </td>
              <td className="log-table__cell">
                {this.renderRangeID(event.event.other_range_id)}
              </td>
              <td className="log-table__cell">
                {this.renderLogInfo(event.pretty_info)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  };

  render() {
    const { log } = this.props;

    return (
      <div>
        <h2 className="base-heading">Range Log</h2>
        <Loading
          loading={!log || log.inFlight}
          error={log && log.lastError}
          render={this.renderContent}
        />
      </div>
    );
  }
}
