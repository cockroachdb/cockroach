// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { TimeScale } from "@cockroachlabs/cluster-ui";

import { PayloadAction } from "src/interfaces/action";

export const TRACK_STATEMENTS_SEARCH =
  "cockroachui/analytics/TRACK_STATEMENTS_SEARCH";
export const TRACK_STATEMENTS_PAGINATION =
  "cockroachui/analytics/TRACK_STATEMENTS_PAGINATION";
export const TRACK_TABLE_SORT = "cockroachui/analytics/TRACK_TABLE_SORT";
export const TRACK_DOWNLOAD_DIAGNOSTIC_BUNDLE =
  "cockroachui/analytics/TRACK_DOWNLOAD_DIAGNOSTIC_BUNDLE";
export const TRACK_CANCEL_DIAGNOSTIC_BUNDLE =
  "cockroachui/analytics/TRACK_CANCEL_DIAGNOSTIC_BUNDLE";
export const TRACK_STATEMENT_DETAILS_SUBNAV_SELECTION =
  "cockroachui/analytics/TRACK_STATEMENT_DETAILS_SUBNAV_SELECTION";
export const TRACK_APPLY_SEARCH_CRITERIA =
  "cockroachui/analytics/TRACK_APPLY_SEARCH_CRITERIA";
export interface TableSortActionPayload {
  tableName: string;
  columnName: string;
  ascending?: boolean;
}

export interface ApplySearchCriteriaPayload {
  ts: TimeScale;
  limit: number;
  sort: string;
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

export function trackCancelDiagnosticsBundleAction(
  statementFingerprint: string,
): PayloadAction<string> {
  return {
    type: TRACK_CANCEL_DIAGNOSTIC_BUNDLE,
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

export function trackApplySearchCriteriaAction(
  ts: TimeScale,
  limit: number,
  sort: string,
): PayloadAction<ApplySearchCriteriaPayload> {
  return {
    type: TRACK_APPLY_SEARCH_CRITERIA,
    payload: {
      ts,
      limit,
      sort,
    },
  };
}
