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
    return `n${rep.node_id} s${rep.store_id} r${rangeID.toString()}/${rep.replica_id}`;
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
  timestamp: protos.cockroach.util.hlc.ITimestamp |
    protos.google.protobuf.ITimestamp,
) {
  let time: moment.Moment = null;
  if (_.has(timestamp, "wall_time")) {
    time = LongToMoment((timestamp as protos.cockroach.util.hlc.ITimestamp).wall_time);
  } else if (_.has(timestamp, "seconds") || _.has(timestamp, "nanos")) {
    time = TimestampToMoment((timestamp as protos.google.protobuf.ITimestamp));
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

export default {
  Duration: PrintDuration,
  ReplicaID: PrintReplicaID,
  Time: PrintTime,
  Timestamp: PrintTimestamp,
  TimestampDelta: PrintTimestampDelta,
};
