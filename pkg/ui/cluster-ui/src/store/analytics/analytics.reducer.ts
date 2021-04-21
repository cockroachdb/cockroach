import { createAction } from "@reduxjs/toolkit";
import { DOMAIN_NAME } from "../utils";

type Page =
  | "Statements"
  | "Statement Details"
  | "Sessions"
  | "Sessions Details";

type SearchEvent = {
  name: "Keyword Searched";
  page: Page;
};

type SortingEvent = {
  name: "Column Sorted";
  page: Page;
  tableName: string;
  columnName: string;
};

type StatementDiagnosticEvent = {
  name: "Statement Diagnostics Clicked";
  page: Page;
  action: "Activated" | "Downloaded";
};

type TabChangedEvent = {
  name: "Tab Changed";
  tabName: string;
  page: Page;
};

type BackButtonClick = {
  name: "Back Clicked";
  page: Page;
};

type StatementClicked = {
  name: "Statement Clicked";
  page: Page;
};

type SessionClicked = {
  name: "Session Clicked";
  page: Page;
};

type SessionActionsClicked = {
  name: "Session Actions Clicked";
  page: Page;
  action: "Terminate Statement" | "Terminate Session";
};

type FilterEvent = {
  name: "Filter Clicked";
  page: Page;
  filterName: string;
  value: string;
};

type AnalyticsEvent =
  | SortingEvent
  | StatementDiagnosticEvent
  | SearchEvent
  | TabChangedEvent
  | BackButtonClick
  | FilterEvent
  | StatementClicked
  | SessionClicked
  | SessionActionsClicked;

const PREFIX = `${DOMAIN_NAME}/analytics`;

/**
 * actions accept payload with "page" field which specifies the page where
 * action occurs and a value expected expected by specific action.
 */
export const actions = {
  track: createAction(`${PREFIX}/track`, (event: AnalyticsEvent) => ({
    payload: event,
  })),
};
