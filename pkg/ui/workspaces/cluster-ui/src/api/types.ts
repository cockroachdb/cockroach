// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

export type PaginationRequest = {
  pageSize: number;
  pageNum: number;
};

export type PaginationState = {
  pageSize: number;
  pageNum: number;
  totalResults: number;
};

export type ResultsWithPagination<T> = {
  results: T;
  pagination: PaginationState;
};
