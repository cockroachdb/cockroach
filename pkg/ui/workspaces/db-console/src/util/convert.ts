// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";

import * as protos from "src/js/protos";

/**
 * NanoToMilli converts a nanoseconds value into milliseconds.
 */
export function NanoToMilli(nano: number): number {
  return nano / 1.0e6;
}

export function MilliToSeconds(milli: number): number {
  return milli / 1.0e3;
}

/**
 * NanoToSeconds converts a nanoseconds value into seconds.
 */
export function NanoToSeconds(nano: number): number {
  return nano / 1.0e9;
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
 * TimestampToNumber converts a Timestamp$Properties object, as seen in wire.proto, to
 * its unix time. If timestamp is null, it returns the `defaultIfNull` value which is
 * by default is current time.
 */
export function TimestampToNumber(
  timestamp?: protos.google.protobuf.ITimestamp,
  defaultIfNull = moment.utc().unix(),
): number {
  if (!timestamp) {
    return defaultIfNull;
  }
  return timestamp.seconds.toNumber() + NanoToMilli(timestamp.nanos) * 1e-3;
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

// durationFromISO8601String function converts a string date in ISO8601 format to moment.Duration
export const durationFromISO8601String = (value: string): moment.Duration => {
  if (!value) {
    return undefined;
  }
  value = value.toUpperCase();
  if (!value.startsWith("P")) {
    value = `PT${value}`;
  }
  return moment.duration(value);
};
