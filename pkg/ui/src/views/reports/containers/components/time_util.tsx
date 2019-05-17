// Copyright 2019 The Cockroach Authors.
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

import * as protos from  "src/js/protos";

export function compareTimestamps(tsa: protos.google.protobuf.ITimestamp,
                                  tsb: protos.google.protobuf.ITimestamp) {
  if (tsa && !tsb) { return 1; }
  else if (!tsa && tsb) { return -1; }
  else if (!tsa && !tsb) { return 0; }
  else if (tsa.seconds.gt(tsb.seconds)) { return 1; }
  else if (tsb.seconds.gt(tsa.seconds)) { return -1; }
  else if (tsa.nanos > tsb.nanos) { return 1; }
  else if (tsa.nanos < tsb.nanos) { return -1; }
  return 0;
}

export function subtractTimestamps(tsa: protos.google.protobuf.ITimestamp,
                                   tsb: protos.google.protobuf.ITimestamp) {
  if (tsb.nanos > tsa.nanos) {
    return new protos.google.protobuf.Duration({
      seconds: tsa.seconds.sub(tsb.seconds).sub(1),
      nanos:   tsa.nanos + 1E9 - tsb.nanos,
    });
  }
  return new protos.google.protobuf.Duration({
    seconds: tsa.seconds.sub(tsb.seconds),
    nanos:   tsa.nanos - tsb.nanos,
  });
}

export function durationAsNumber(dur: protos.google.protobuf.IDuration) {
  return dur.seconds.toNumber() + dur.nanos / 1E9;
}

export function addDuration(ts: protos.google.protobuf.ITimestamp,
                            duration: protos.google.protobuf.IDuration) {
  if (ts.nanos + duration.nanos > 1E9) {
    return new protos.google.protobuf.Timestamp({
      seconds: ts.seconds.add(duration.seconds).add(1), nanos: (ts.nanos + duration.nanos) % 1E9});
  } else {
    return new protos.google.protobuf.Timestamp({
      seconds: ts.seconds.add(duration.seconds), nanos: ts.nanos + duration.nanos});
  }
}

export function formatNumber(num: number, precision?: number) {
  const numStr: string = num.toString();
  const idxOfDot: number = numStr.indexOf(".")
  const intStr: string = numStr.substring(0, idxOfDot == -1 ? numStr.length : idxOfDot);
  const decStr: string = numStr.substring(idxOfDot == -1 ? numStr.length : idxOfDot);
  const startIdx: number = intStr.length % 3 == 0 ? 3 : intStr.length % 3;
  var lastIdx: number = 0;
  var fmt: string = "";
  for (let i = startIdx; i <= intStr.length; i+=3) {
    fmt += intStr.substring(lastIdx, i);
    if (i < intStr.length) {
      fmt += ",";
    }
    lastIdx = i;
  }
  return fmt + (precision ? decStr.substring(0, Math.min(decStr.length, precision + 1)) : decStr);
}

export function formatDuration(duration: protos.google.protobuf.IDuration, truncate: boolean) {
  const {seconds, nanos} = duration;
  if (seconds.lt(0)) {
    return "negative";
  } else if (seconds.gt(0)) {
    return formatNumber(seconds.toNumber()) + "." + nanos.toString().padStart(9, 0).substring(0, truncate ? 3 : 9) + "s";
  } else if (nanos > 1000000) {
    return (truncate ? formatNumber(Math.floor(nanos / 1000) / 1000) : formatNumber(nanos / 1000000)) + "ms";
  } else if (nanos > 1000) {
    return formatNumber(nanos / 1000) + "µs";
  }
  return formatNumber(nanos) + "ns";
}

export function formatDateTime(ts: protos.google.protobuf.ITimestamp, truncate: boolean) {
  const date: any = timestampToDate(ts);
  return date.getHours().toString().padStart(2, '0') + ":" +
    date.getMinutes().toString().padStart(2, '0') + ":" +
    date.getSeconds().toString().padStart(2, '0') + "." +
    (truncate ? date.getMilliseconds().toString().padStart(3, '0') : ts.nanos.toString().padStart(9, 0));
}

export function timestampToDate(ts: protos.google.protobuf.ITimestamp) {
  return new Date(ts.seconds.toNumber() * 1000 + Math.floor(ts.nanos / 1000000));
}

