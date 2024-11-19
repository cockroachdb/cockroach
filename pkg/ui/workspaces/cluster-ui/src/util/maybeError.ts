// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import isError from "lodash/isError";
import isString from "lodash/isString";

export const maybeError = (
  e: unknown,
  fallbackMsg = "An unexpected error has occurred.",
): Error => {
  if (isError(e)) {
    return e;
  }
  if (isString(e)) {
    return new Error(e);
  }
  return new Error(fallbackMsg);
};
