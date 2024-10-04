// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { APIV2PaginationResponse, PaginationState } from "./types";

export const convertServerPaginationToClientPagination = (
  pagination: APIV2PaginationResponse,
): PaginationState => {
  return {
    pageNum: pagination?.page_num ?? 0,
    pageSize: pagination?.page_size ?? 0,
    totalResults: pagination?.total_results ?? 0,
  };
};
