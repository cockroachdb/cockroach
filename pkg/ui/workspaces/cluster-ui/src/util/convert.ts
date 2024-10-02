// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { fromNumber } from "long";
import moment from "moment-timezone";

export type Timestamp = protos.google.protobuf.ITimestamp;

export const minDate = moment.utc("0001-01-01"); // minimum value as per UTC.

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
 * TimestampToString converts a Timestamp$Properties object, as seen in wire.proto, to
 * its unix time and returns that value as a string. If timestamp is null, it returns
 * the `defaultIfNull` value which is by default is current time.
 */
export function TimestampToString(
  timestamp?: protos.google.protobuf.ITimestamp,
  defaultIfNull = moment.utc().unix(),
): string {
  if (!timestamp) {
    return defaultIfNull.toString();
  }
  return (
    timestamp.seconds.toNumber() +
    NanoToMilli(timestamp.nanos) * 1e-3
  ).toString();
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

/**
 * DurationToNumber converts a Duration object, as seen in wire.proto, to
 * a number representing the duration in seconds. If timestamp is null,
 * it returns the `defaultIfNull` value which is by default 0.
 */
export function DurationToNumber(
  duration?: protos.google.protobuf.IDuration,
  defaultIfNull = 0,
): number {
  if (!duration || !duration?.seconds) {
    return defaultIfNull;
  }
  return duration.seconds.toNumber() + NanoToMilli(duration.nanos) * 1e-3;
}

/**
 * DurationToMomentDuration converts a Duration object, as seen in wire.proto,
 * to a duration object from momentjs. If timestamp is null it returns the `defaultIfNull`
 * value which is by default 0, as momentjs duration.
 */
export function DurationToMomentDuration(
  duration?: protos.google.protobuf.IDuration,
  defaultIfNullSeconds = 0,
): moment.Duration {
  if (!duration || !duration?.seconds) {
    return moment.duration(defaultIfNullSeconds, "seconds");
  }

  const seconds = duration.seconds.toNumber() + duration.nanos * 1e-9;
  return moment.duration(seconds, "seconds");
}

/**
 * NumberToDuration converts a number representing a duration in seconds
 * to a Duration object.
 */
export function NumberToDuration(
  seconds?: number,
): protos.google.protobuf.IDuration {
  return new protos.google.protobuf.Duration({
    seconds: fromNumber(seconds),
    nanos: SecondsToNano(seconds - Math.floor(seconds)),
  });
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

export function makeTimestamp(unixTs: number): Timestamp {
  return new protos.google.protobuf.Timestamp({
    seconds: fromNumber(unixTs),
  });
}

export function stringToTimestamp(t: string): Timestamp {
  const unix = new Date(t).getTime() / 1000;
  return makeTimestamp(unix);
}
