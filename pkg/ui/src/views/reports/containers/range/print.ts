import _ from "lodash";
import Long from "long";
import moment from "moment";

import * as protos from "src/js/protos";
import { LongToMoment, TimestampToMoment } from "src/util/convert";

export const dateFormat = "Y-MM-DD HH:mm:ss";

export function PrintReplicaID(rangeID: Long, rep: protos.cockroach.roachpb.ReplicaDescriptor$Properties) {
  return `n${rep.node_id} s${rep.store_id} r${rangeID.toString()}/${rep.replica_id}`;
}

export function PrintTime(time: moment.Moment) {
  return time.format(dateFormat);
}

export function PrintTimestamp(
  timestamp: protos.cockroach.util.hlc.Timestamp$Properties |
    protos.google.protobuf.Timestamp$Properties,
) {
  let time: moment.Moment = null;
  if (_.has(timestamp, "wall_time")) {
    time = LongToMoment((timestamp as protos.cockroach.util.hlc.Timestamp$Properties).wall_time);
  } else if (_.has(timestamp, "seconds") || _.has(timestamp, "nanos")) {
    time = TimestampToMoment((timestamp as protos.google.protobuf.Timestamp$Properties));
  } else {
    return "";
  }
  return PrintTime(time);
}

export function PrintDuration(duration: moment.Duration) {
  const results: string[] = [];
  if (duration.days() > 0) {
    results.push(`${duration.days()}days`);
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
  newTimestamp: protos.cockroach.util.hlc.Timestamp$Properties,
  oldTimestamp: protos.cockroach.util.hlc.Timestamp$Properties,
) {
  if (_.isNil(oldTimestamp) || _.isNil(newTimestamp)) {
    return "";
  }
  const newTime = LongToMoment(newTimestamp.wall_time);
  const oldTime = LongToMoment(oldTimestamp.wall_time);
  const diff = moment.duration(newTime.diff(oldTime));
  return this.Duration(diff);
}

export default {
  Duration: PrintDuration,
  ReplicaID: PrintReplicaID,
  Time: PrintTime,
  Timestamp: PrintTimestamp,
  TimestampDelta: PrintTimestampDelta,
};
