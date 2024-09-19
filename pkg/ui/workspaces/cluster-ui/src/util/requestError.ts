// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
