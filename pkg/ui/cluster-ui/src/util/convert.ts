import moment from "moment";

import * as protos from "@cockroachlabs/crdb-protobuf-client";

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
 * SecondsToNano converts a second value into nanoseconds.
 */
export function SecondsToNano(sec: number): number {
  return sec * 1.0e9;
}

/**
 * TimestampToMoment converts a Timestamp$Properties object, as seen in wire.proto, to
 * a Moment object. If timestamp is null, it returns the `defaultsIfNull` value which is
 * by default is current time.
 */
export function TimestampToMoment(
  timestamp?: protos.google.protobuf.ITimestamp,
  defaultsIfNull = moment.utc(),
): moment.Moment {
  if (!timestamp) {
    return defaultsIfNull;
  }
  return moment.utc(
    timestamp.seconds.toNumber() * 1e3 + NanoToMilli(timestamp.nanos),
  );
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
