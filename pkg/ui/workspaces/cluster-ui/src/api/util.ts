// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment-timezone";

export const STATUS_PREFIX = "_status";
export const ADMIN_API_PREFIX = "_admin/v1";
export const PROMISE_TIMEOUT = moment.duration(30, "s"); // seconds

// withTimeout wraps a promise in a timeout (cribbed from db-console).
export function withTimeout<T>(
  promise: Promise<T>,
  timeout?: moment.Duration,
): Promise<T> {
  if (timeout) {
    return new Promise<T>((resolve, reject) => {
      setTimeout(
        () => reject(new TimeoutError(timeout)),
        timeout.asMilliseconds(),
      );
      promise.then(resolve, reject);
    });
  } else {
    return promise;
  }
}

export class TimeoutError extends Error {
  timeout: moment.Duration;
  constructor(timeout: moment.Duration) {
    const message = `Promise timed out after ${timeout.asMilliseconds()} ms`;
    super(message);

    this.name = this.constructor.name;
    this.timeout = timeout;
  }
}

export const fromHexString = (hexString: string): Uint8Array =>
  Uint8Array.from(hexString.match(/.{1,2}/g).map(byte => parseInt(byte, 16)));
