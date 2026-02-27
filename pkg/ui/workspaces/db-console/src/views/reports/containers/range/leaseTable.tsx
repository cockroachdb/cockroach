// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import head from "lodash/head";
import isEmpty from "lodash/isEmpty";
import isNil from "lodash/isNil";
import map from "lodash/map";
import reverse from "lodash/reverse";
import React from "react";

import * as protos from "src/js/protos";
import Lease from "src/views/reports/containers/range/lease";
import Print from "src/views/reports/containers/range/print";
import RangeInfo from "src/views/reports/containers/range/rangeInfo";

interface LeaseTableProps {
  info: protos.cockroach.server.serverpb.IRangeInfo;
}

function renderLeaseCell(value: string, title = "") {
  if (isEmpty(title)) {
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

function renderLeaseTimestampCell(
  timestamp: protos.cockroach.util.hlc.ITimestamp,
) {
  if (isNil(timestamp)) {
    return renderLeaseCell("<no value>");
  }

  const value = Print.Timestamp(timestamp);
  return renderLeaseCell(value, `${value}\n${timestamp.wall_time.toString()}`);
}

export default function LeaseTable({
  info,
}: LeaseTableProps): React.ReactElement {
  // TODO(bram): Maybe search for the latest lease record instead of just trusting the
  // leader?
  const rangeID = info.state.state.desc.range_id;
  const header = (
    <h2 className="base-heading">
      Lease History (from{" "}
      {Print.ReplicaID(rangeID, RangeInfo.GetLocalReplica(info))})
    </h2>
  );
  if (isEmpty(info.lease_history)) {
    return (
      <div>
        {header}
        <h3>There is no lease history for this range</h3>
      </div>
    );
  }

  const isEpoch = Lease.IsEpoch(head(info.lease_history));
  const leaseHistory = reverse(info.lease_history);
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
          {map(leaseHistory, (lease, key) => {
            let prevProposedTimestamp: protos.cockroach.util.hlc.ITimestamp =
              null;
            let prevStart: protos.cockroach.util.hlc.ITimestamp = null;
            if (key < leaseHistory.length - 1) {
              prevProposedTimestamp = leaseHistory[key + 1].proposed_ts;
              prevStart = leaseHistory[key + 1].start;
            }
            return (
              <tr key={key} className="lease-table__row">
                {renderLeaseCell(Print.ReplicaID(rangeID, lease.replica))}
                {isEpoch
                  ? renderLeaseCell(
                      `n${lease.replica.node_id}, ${lease.epoch.toString()}`,
                    )
                  : null}
                {renderLeaseTimestampCell(lease.proposed_ts)}
                {renderLeaseCell(
                  Print.TimestampDelta(
                    lease.proposed_ts,
                    prevProposedTimestamp,
                  ),
                )}
                {!isEpoch ? renderLeaseTimestampCell(lease.expiration) : null}
                {renderLeaseTimestampCell(lease.start)}
                {renderLeaseCell(Print.TimestampDelta(lease.start, prevStart))}
                {renderLeaseCell(
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
