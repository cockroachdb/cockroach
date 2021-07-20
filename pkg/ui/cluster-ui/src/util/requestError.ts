// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

export class RequestError extends Error {
  status: number;
  constructor(statusText: string, status: number, message?: string) {
    super(statusText);
    this.status = status;
    this.name = "RequestError";
    this.message = message;
  }
}

export function isRequestError(
  error: Error | RequestError,
): error is RequestError {
  return "status" in error && error.name === "RequestError";
}

export function isForbiddenRequestError(error: Error): boolean {
  return isRequestError(error) && error.status === 403; // match to HTTP Forbidden status code
}
