// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createAction } from "@reduxjs/toolkit";

import { DOMAIN_NAME } from "../utils";

type Page =
  | "Databases"
  | "Database Details"
  | "Index Details"
  | "Jobs"
  | "Schema Insights"
  | "Sessions"
  | "Sessions Details"
  | "Statements"
  | "Statement Details"
  | "Statement Insight Details"
  | "Transactions"
  | "Transaction Details"
  | "Transaction Insight Details"
  | "Workload Insights - Statement"
  | "Workload Insights - Transaction";

type ApplySearchCriteriaEvent = {
  name: "Apply Search Criteria";
  page: Page;
  tsValue: string;
  limitValue: number;
  sortValue: string;
};

type BackButtonClick = {
  name: "Back Clicked";
  page: Page;
};

type ColumnsChangeEvent = {
  name: "Columns Selected change";
  page: Page;
  value: string;
};

type FilterEvent = {
  name: "Filter Clicked";
  page: Page;
  filterName: string;
  value: string;
};

type JobTypeEvent = {
  name: "Job Type Selected";
  page: Page;
  value: string;
};

type ResetStats = {
  name: "Reset Index Usage" | "Reset Stats";
  page: Page;
};

type SearchEvent = {
  name: "Keyword Searched";
  page: Page;
};

type SessionActionsClicked = {
  name: "Session Actions Clicked";
  page: Page;
  action: "Cancel Statement" | "Cancel Session";
};

type SessionClicked = {
  name: "Session Clicked";
  page: Page;
};

type SortingEvent = {
  name: "Column Sorted";
  page: Page;
  tableName: string;
  columnName: string;
};

type StatementClicked = {
  name: "Statement Clicked";
  page: Page;
};

type StatementDiagnosticEvent = {
  name: "Statement Diagnostics Clicked";
  page: Page;
  action: "Activated" | "Downloaded" | "Cancelled";
};

type TabChangedEvent = {
  name: "Tab Changed";
  tabName: string;
  page: Page;
};

type TimeScaleChangeEvent = {
  name: "TimeScale changed";
  page: Page;
  value: string;
};

type ViewModeEvent = {
  name: "View Mode Clicked";
  page: Page;
  value: string;
};

type AutoRefreshEvent = {
  name: "Auto Refresh Toggle";
  page: Page;
  value: boolean;
};

type ManualRefreshEvent = {
  name: "Manual Refresh";
  page: Page;
};

type AnalyticsEvent =
  | ApplySearchCriteriaEvent
  | BackButtonClick
  | ColumnsChangeEvent
  | FilterEvent
  | JobTypeEvent
  | ResetStats
  | SearchEvent
  | SessionActionsClicked
  | SessionClicked
  | SortingEvent
  | StatementClicked
  | StatementDiagnosticEvent
  | TabChangedEvent
  | TimeScaleChangeEvent
  | ViewModeEvent
  | AutoRefreshEvent
  | ManualRefreshEvent;

const PREFIX = `${DOMAIN_NAME}/analytics`;

/**
 * actions accept payload with "page" field which specifies the page where
 * action occurs and a value expected by specific action.
 */
export const actions = {
  track: createAction(`${PREFIX}/track`, (event: AnalyticsEvent) => ({
    payload: event,
  })),
};
