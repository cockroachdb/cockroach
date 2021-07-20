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
import Long from "long";
import moment from "moment";

import * as protos from "src/js/protos";
import { LongToMoment, TimestampToMoment } from "src/util/convert";

export const dateFormat = "Y-MM-DD HH:mm:ss";

// PrintReplicaID prints our standard replica identifier. If the replica is nil,
// it uses passed in store, node and replica IDs instead.
export function PrintReplicaID(
  rangeID: Long,
  rep: protos.cockroach.roachpb.IReplicaDescriptor,
  nodeID?: number,
  storeID?: number,
  replicaID?: Long,
) {
  if (!_.isNil(rep)) {
    return `n${rep.node_id} s${rep.store_id} r${rangeID.toString()}/${
      rep.replica_id
    }`;
  }
  // Fall back to the passed in node, store and replica IDs. If those are nil,
  // use a question mark instead.
  const nodeIDString = _.isNil(nodeID) ? "?" : nodeID.toString();
  const storeIDString = _.isNil(storeID) ? "?" : storeID.toString();
  const replicaIDString = _.isNil(replicaID) ? "?" : replicaID.toString();
  return `n${nodeIDString} s${storeIDString} r${rangeID.toString()}/${replicaIDString}`;
}

export function PrintTime(time: moment.Moment) {
  return time.format(dateFormat);
}

export function PrintTimestamp(
  timestamp:
    | protos.cockroach.util.hlc.ITimestamp
    | protos.google.protobuf.ITimestamp,
) {
  let time: moment.Moment = null;
  if (_.has(timestamp, "wall_time")) {
    time = LongToMoment(
      (timestamp as protos.cockroach.util.hlc.ITimestamp).wall_time,
    );
  } else if (_.has(timestamp, "seconds") || _.has(timestamp, "nanos")) {
    time = TimestampToMoment(timestamp as protos.google.protobuf.ITimestamp);
  } else {
    return "";
  }
  return PrintTime(time);
}

export function PrintDuration(duration: moment.Duration) {
  const results: string[] = [];
  if (duration.days() > 0) {
    results.push(`${duration.days()}d`);
  }
  if (duration.hours() > 0) {
    results.push(`${duration.hours()}h`);
  }
  if (duration.minutes() > 0) {
    results.push(`${duration.minutes()}m`);
  }
  if (duration.seconds() > 0) {
    results.push(`${duration.seconds()}s`);
  }
  const ms = _.round(duration.milliseconds());
  if (ms > 0) {
    results.push(`${ms}ms`);
  }
  if (_.isEmpty(results)) {
    return "0s";
  }
  return _.join(results, " ");
}

export function PrintTimestampDelta(
  newTimestamp: protos.cockroach.util.hlc.ITimestamp,
  oldTimestamp: protos.cockroach.util.hlc.ITimestamp,
) {
  if (_.isNil(oldTimestamp) || _.isNil(newTimestamp)) {
    return "";
  }
  const newTime = LongToMoment(newTimestamp.wall_time);
  const oldTime = LongToMoment(oldTimestamp.wall_time);
  const diff = moment.duration(newTime.diff(oldTime));
  return PrintDuration(diff);
}

// PrintTimestampDeltaFromNow is like PrintTimestampDelta, except it works both
// when `timestamp` is below or above `now`, and at appends "ago" or "in the
// future" to the result.
export function PrintTimestampDeltaFromNow(
  timestamp: protos.cockroach.util.hlc.ITimestamp,
  now: moment.Moment,
): string {
  if (_.isNil(timestamp)) {
    return "";
  }
  const time: moment.Moment = LongToMoment(timestamp.wall_time);
  if (now.isAfter(time)) {
    const diff = moment.duration(now.diff(time));
    return `${PrintDuration(diff)} ago`;
  }
  const diff = moment.duration(time.diff(now));
  return `${PrintDuration(diff)} in the future`;
}

export default {
  Duration: PrintDuration,
  ReplicaID: PrintReplicaID,
  Time: PrintTime,
  Timestamp: PrintTimestamp,
  TimestampDelta: PrintTimestampDelta,
  TimestampDeltaFromNow: PrintTimestampDeltaFromNow,
};
