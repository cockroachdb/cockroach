import { createAction } from "@reduxjs/toolkit";

type Page =
  | "statements"
  | "statementDetails"
  | "transactions"
  | "transactionDetails";

type PagePayload<T> = {
  page: Page;
  value: T;
};

type SortingPayload = {
  tableName: string;
  columnName: string;
  ascending?: boolean;
};

const PREFIX = "adminUI/analytics";

/**
 * actions accept payload with "page" field which specifies the page where
 * action occurs and a value expected expected by specific action.
 */
export const actions = {
  search: createAction<PagePayload<number>>(`${PREFIX}/search`),
  pagination: createAction<PagePayload<number>>(`${PREFIX}/pagination`),
  sorting: createAction<PagePayload<SortingPayload>>(`${PREFIX}/sorting`),
  activateDiagnostics: createAction<PagePayload<string>>(
    `${PREFIX}/activateStatementDiagnostics`,
  ),
  downloadStatementDiagnostics: createAction<PagePayload<string>>(
    `${PREFIX}/downloadStatementDiagnostics`,
  ),
  subNavigationSelection: createAction<PagePayload<string>>(
    `${PREFIX}/subNavigationSelection`,
  ),
};
