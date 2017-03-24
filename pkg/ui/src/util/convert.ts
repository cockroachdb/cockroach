import moment from "moment";

import * as protos from "../js/protos";

type Timestamp$Properties = protos.cockroach.server.serverpb.EventsResponse.Event.Timestamp$Properties | protos.cockroach.server.serverpb.GetUIDataResponse.Timestamp$Properties;

/**
 * NanoToMilli converts a nanoseconds value into milliseconds.
 */
export function NanoToMilli(nano: number): number {
  return nano / 1.0e6;
}

/**
 * MilliToNano converts a millisecond value into nanoseconds.
 */
export function MilliToNano(milli: number): number {
  return milli * 1.0e6;
}

/**
 * TimestampToMoment converts a Timestamp$Properties object, as seen in wire.proto, to
 * a Moment object. If timestamp is null, it returns the current time.
 */
export function TimestampToMoment(timestamp?: Timestamp$Properties): moment.Moment {
  if (!timestamp) {
    return moment.utc();
  }
  return moment.utc((timestamp.sec.toNumber() * 1e3) + NanoToMilli(timestamp.nsec));
}

/**
 * LongToMoment converts a Long, representing nanos since the epoch, to a Moment
 * object. If timestamp is null, it returns the current time.
 */
export function LongToMoment(timestamp: Long): moment.Moment {
  if (!timestamp) {
    return moment.utc();
  }
  return moment.utc(NanoToMilli(timestamp.toNumber()));
}
