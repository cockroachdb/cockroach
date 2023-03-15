// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";

export type RequestState<DataType> = {
  data: DataType | null;
  error: Error | null;
  valid: boolean;
  inFlight: boolean;
  lastUpdated: moment.Moment | null;
};

export function createInitialState<T>(
  overrides?: Partial<RequestState<T>>,
): RequestState<T> {
  return {
    data: null,
    error: null,
    valid: false,
    lastUpdated: null,
    inFlight: false,
    ...overrides,
  };
}
