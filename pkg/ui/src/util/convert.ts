// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

import moment from "moment";

import * as protos from "src/js/protos";

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
export function TimestampToMoment(timestamp?: protos.google.protobuf.ITimestamp): moment.Moment {
  if (!timestamp) {
    return moment.utc();
  }
  return moment.utc((timestamp.seconds.toNumber() * 1e3) + NanoToMilli(timestamp.nanos));
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
