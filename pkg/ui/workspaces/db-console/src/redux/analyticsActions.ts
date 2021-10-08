// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "src/interfaces/action";

export const TRACK_STATEMENTS_SEARCH =
  "cockroachui/analytics/TRACK_STATEMENTS_SEARCH";
export const TRACK_STATEMENTS_PAGINATION =
  "cockroachui/analytics/TRACK_STATEMENTS_PAGINATION";
export const TRACK_TABLE_SORT = "cockroachui/analytics/TRACK_TABLE_SORT";
export const TRACK_DOWNLOAD_DIAGNOSTIC_BUNDLE =
  "cockroachui/analytics/TRACK_DOWNLOAD_DIAGNOSTIC_BUNDLE";
export const TRACK_STATEMENT_DETAILS_SUBNAV_SELECTION =
  "cockroachui/analytics/TRACK_STATEMENT_DETAILS_SUBNAV_SELECTION";

export interface TableSortActionPayload {
  tableName: string;
  columnName: string;
  ascending?: boolean;
}

export function trackStatementsSearchAction(
  searchResults: number,
): PayloadAction<number> {
  return {
    type: TRACK_STATEMENTS_SEARCH,
    payload: searchResults,
  };
}

export function trackStatementsPaginationAction(
  pageNum: number,
): PayloadAction<number> {
  return {
    type: TRACK_STATEMENTS_PAGINATION,
    payload: pageNum,
  };
}

export function trackTableSortAction(
  tableName: string,
  columnName: string,
  ascending?: boolean,
): PayloadAction<TableSortActionPayload> {
  return {
    type: TRACK_TABLE_SORT,
    payload: {
      tableName,
      columnName,
      ascending,
    },
  };
}

export function trackDownloadDiagnosticsBundleAction(
  statementFingerprint: string,
): PayloadAction<string> {
  return {
    type: TRACK_DOWNLOAD_DIAGNOSTIC_BUNDLE,
    payload: statementFingerprint,
  };
}

export function trackStatementDetailsSubnavSelectionAction(
  tabName: string,
): PayloadAction<string> {
  return {
    type: TRACK_STATEMENT_DETAILS_SUBNAV_SELECTION,
    payload: tabName,
  };
}
