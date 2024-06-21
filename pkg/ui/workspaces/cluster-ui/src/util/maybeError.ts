// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
