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
import Lease from "src/views/reports/containers/range/lease";
import Print from "src/views/reports/containers/range/print";
import RangeInfo from "src/views/reports/containers/range/rangeInfo";

interface LeaseTableProps {
  info: protos.cockroach.server.serverpb.IRangeInfo;
}

export default class LeaseTable extends React.Component<LeaseTableProps, {}> {
  renderLeaseCell(value: string, title: string = "") {
    if (_.isEmpty(title)) {
      return (
        <td className="lease-table__cell" title={value}>
          {value}
        </td>
      );
    }
    return (
      <td className="lease-table__cell" title={title}>
        {value}
      </td>
    );
  }

  renderLeaseTimestampCell(timestamp: protos.cockroach.util.hlc.ITimestamp) {
    if (_.isNil(timestamp)) {
      return this.renderLeaseCell("<no value>");
    }

    const value = Print.Timestamp(timestamp);
    return this.renderLeaseCell(
      value,
      `${value}\n${timestamp.wall_time.toString()}`,
    );
  }

  render() {
    const { info } = this.props;
    // TODO(bram): Maybe search for the latest lease record instead of just trusting the
    // leader?
    const rangeID = info.state.state.desc.range_id;
    const header = (
      <h2 className="base-heading">
        Lease History (from{" "}
        {Print.ReplicaID(rangeID, RangeInfo.GetLocalReplica(info))})
      </h2>
    );
    if (_.isEmpty(info.lease_history)) {
      return (
        <div>
          {header}
          <h3>There is no lease history for this range</h3>
        </div>
      );
    }

    const isEpoch = Lease.IsEpoch(_.head(info.lease_history));
    const leaseHistory = _.reverse(info.lease_history);
    return (
      <div>
        {header}
        <table className="lease-table">
          <tbody>
            <tr className="lease-table__row lease-table__row--header">
              <th className="lease-table__cell lease-table__cell--header">
                Replica
              </th>
              {isEpoch ? (
                <th className="lease-table__cell lease-table__cell--header">
                  Epoch
                </th>
              ) : null}
              <th className="lease-table__cell lease-table__cell--header">
                Proposed
              </th>
              <th className="lease-table__cell lease-table__cell--header">
                Proposed Delta
              </th>
              {!isEpoch ? (
                <th className="lease-table__cell lease-table__cell--header">
                  Expiration
                </th>
              ) : null}
              <th className="lease-table__cell lease-table__cell--header">
                Start
              </th>
              <th className="lease-table__cell lease-table__cell--header">
                Start Delta
              </th>
              <th className="lease-table__cell lease-table__cell--header">
                Acquisition Type
              </th>
            </tr>
            {_.map(leaseHistory, (lease, key) => {
              let prevProposedTimestamp: protos.cockroach.util.hlc.ITimestamp = null;
              let prevStart: protos.cockroach.util.hlc.ITimestamp = null;
              if (key < leaseHistory.length - 1) {
                prevProposedTimestamp = leaseHistory[key + 1].proposed_ts;
                prevStart = leaseHistory[key + 1].start;
              }
              return (
                <tr key={key} className="lease-table__row">
                  {this.renderLeaseCell(
                    Print.ReplicaID(rangeID, lease.replica),
                  )}
                  {isEpoch
                    ? this.renderLeaseCell(
                        `n${lease.replica.node_id}, ${lease.epoch.toString()}`,
                      )
                    : null}
                  {this.renderLeaseTimestampCell(lease.proposed_ts)}
                  {this.renderLeaseCell(
                    Print.TimestampDelta(
                      lease.proposed_ts,
                      prevProposedTimestamp,
                    ),
                  )}
                  {!isEpoch
                    ? this.renderLeaseTimestampCell(lease.expiration)
                    : null}
                  {this.renderLeaseTimestampCell(lease.start)}
                  {this.renderLeaseCell(
                    Print.TimestampDelta(lease.start, prevStart),
                  )}
                  {this.renderLeaseCell(
                    protos.cockroach.roachpb.LeaseAcquisitionType[
                      lease.acquisition_type
                    ],
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  }
}
