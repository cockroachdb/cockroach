// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";

import {
  SqlStatsSortType,
  DEFAULT_STATS_REQ_OPTIONS,
} from "src/api/statementsApi";
import { WorkloadInsightEventFilters } from "src/insights";
import { defaultFilters, Filters } from "src/queryFilter/";
import { defaultFiltersForSessionsPage } from "src/sessions/sessionsPage";

import { TimeScale, defaultTimeScaleSelected } from "../../timeScaleDropdown";
import { DOMAIN_NAME } from "../utils";

type SortSetting = {
  ascending: boolean;
  columnTitle: string;
};

export enum LocalStorageKeys {
  GLOBAL_TIME_SCALE = "timeScale/SQLActivity",
  STMT_FINGERPRINTS_LIMIT = "limit/StatementsPage",
  STMT_FINGERPRINTS_SORT = "sort/StatementsPage",
  TXN_FINGERPRINTS_LIMIT = "limit/TransactionsPage",
  TXN_FINGERPRINTS_SORT = "sort/TransactionsPage",
  DB_SORT = "sortSetting/DatabasesPage",
  DB_FILTERS = "filters/DatabasesPage",
  DB_SEARCH = "search/DatabasesPage",
  DB_DETAILS_TABLES_PAGE_SORT = "sortSetting/DatabasesDetailsTablesPage",
  DB_DETAILS_TABLES_PAGE_FILTERS = "filters/DatabasesDetailsTablesPage",
  DB_DETAILS_TABLES_PAGE_SEARCH = "search/DatabasesDetailsTablesPage",
  DB_DETAILS_GRANTS_PAGE_SORT = "sortSetting/DatabasesDetailsGrantsPage",
  DB_DETAILS_VIEW_MODE = "viewMode/DatabasesDetailsPage",
  ACTIVE_EXECUTIONS_IS_AUTOREFRESH_ENABLED = "isAutoRefreshEnabled/ActiveExecutions",
}

export type LocalStorageState = {
  "adminUi/showDiagnosticsModal": boolean;
  "showColumns/ActiveTransactionsPage": string;
  "showColumns/StatementsPage": string;
  "showColumns/TransactionPage": string;
  "showColumns/SessionsPage": string;
  "showColumns/StatementInsightsPage": string;
  "showColumns/JobsPage": string;
  [LocalStorageKeys.GLOBAL_TIME_SCALE]: TimeScale;
  [LocalStorageKeys.STMT_FINGERPRINTS_LIMIT]: number;
  [LocalStorageKeys.STMT_FINGERPRINTS_SORT]: SqlStatsSortType;
  [LocalStorageKeys.TXN_FINGERPRINTS_LIMIT]: number;
  [LocalStorageKeys.TXN_FINGERPRINTS_SORT]: SqlStatsSortType;
  "sortSetting/ActiveTransactionsPage": SortSetting;
  "sortSetting/StatementsPage": SortSetting;
  "sortSetting/TransactionsPage": SortSetting;
  "sortSetting/SessionsPage": SortSetting;
  "sortSetting/JobsPage": SortSetting;
  "sortSetting/InsightsPage": SortSetting;
  "sortSetting/SchemaInsightsPage": SortSetting;
  [LocalStorageKeys.DB_SORT]: SortSetting;
  [LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SORT]: SortSetting;
  [LocalStorageKeys.DB_DETAILS_GRANTS_PAGE_SORT]: SortSetting;
  "filters/ActiveTransactionsPage": Filters;
  "filters/StatementsPage": Filters;
  "filters/TransactionsPage": Filters;
  [LocalStorageKeys.DB_FILTERS]: Filters;
  "filters/SessionsPage": Filters;
  "filters/InsightsPage": WorkloadInsightEventFilters;
  "filters/SchemaInsightsPage": Filters;
  [LocalStorageKeys.DB_DETAILS_TABLES_PAGE_FILTERS]: Filters;
  "search/StatementsPage": string;
  "search/TransactionsPage": string;
  [LocalStorageKeys.DB_SEARCH]: string;
  [LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SEARCH]: string;
  "typeSetting/JobsPage": number;
  "statusSetting/JobsPage": string;
  "showSetting/JobsPage": string;
  [LocalStorageKeys.ACTIVE_EXECUTIONS_IS_AUTOREFRESH_ENABLED]: boolean;
  "requestTime/StatementsPage": moment.Moment;
  "requestTime/TransactionsPage": moment.Moment;
};

type Payload = {
  key: keyof LocalStorageState;
  value: any;
};

export type TypedPayload<T> = {
  value: T;
};

const defaultSortSetting: SortSetting = {
  ascending: false,
  columnTitle: "executionCount",
};

const defaultSortSettingActiveExecutions: SortSetting = {
  ascending: false,
  columnTitle: "startTime",
};

const defaultSortSettingInsights: SortSetting = {
  ascending: false,
  columnTitle: "startTime",
};

const defaultSortSettingSchemaInsights: SortSetting = {
  ascending: false,
  columnTitle: "insights",
};

const defaultNameSortSetting: SortSetting = {
  ascending: true,
  columnTitle: "name",
};

const defaultFiltersActiveExecutions = {
  app: "",
  executionStatus: "",
};

const defaultFiltersInsights = {
  app: "",
  workloadInsightType: "",
};

const defaultFiltersSchemaInsights = {
  database: "",
  schemaInsightType: "",
};

const defaultSessionsSortSetting: SortSetting = {
  ascending: false,
  columnTitle: "statementAge",
};

const defaultJobsSortSetting: SortSetting = {
  ascending: false,
  columnTitle: "lastExecutionTime",
};

const defaultJobStatusSetting = "";

const defaultJobShowSetting = "0";

const defaultJobTypeSetting = 0;

const defaultIsAutoRefreshEnabledSetting = true;

const initialState: LocalStorageState = {
  "adminUi/showDiagnosticsModal": false,
  [LocalStorageKeys.STMT_FINGERPRINTS_LIMIT]: DEFAULT_STATS_REQ_OPTIONS.limit,
  [LocalStorageKeys.STMT_FINGERPRINTS_SORT]: DEFAULT_STATS_REQ_OPTIONS.sortStmt,
  "showColumns/ActiveTransactionsPage": null,
  "showColumns/StatementsPage": null,
  "showColumns/TransactionPage": null,
  [LocalStorageKeys.TXN_FINGERPRINTS_LIMIT]: DEFAULT_STATS_REQ_OPTIONS.limit,
  [LocalStorageKeys.TXN_FINGERPRINTS_SORT]: DEFAULT_STATS_REQ_OPTIONS.sortTxn,
  "showColumns/SessionsPage": null,
  "showColumns/StatementInsightsPage": null,
  "showColumns/JobsPage": null,
  "showSetting/JobsPage": defaultJobShowSetting,
  [LocalStorageKeys.GLOBAL_TIME_SCALE]: defaultTimeScaleSelected,
  "sortSetting/ActiveTransactionsPage": defaultSortSettingActiveExecutions,
  "sortSetting/JobsPage": defaultJobsSortSetting,
  "sortSetting/StatementsPage": defaultSortSetting,
  "sortSetting/TransactionsPage": defaultSortSetting,
  "sortSetting/SessionsPage": defaultSessionsSortSetting,
  "sortSetting/InsightsPage": defaultSortSettingInsights,
  "sortSetting/SchemaInsightsPage": defaultSortSettingSchemaInsights,
  [LocalStorageKeys.DB_SORT]: defaultNameSortSetting,
  [LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SORT]: defaultNameSortSetting,
  [LocalStorageKeys.DB_DETAILS_GRANTS_PAGE_SORT]: defaultNameSortSetting,
  "filters/ActiveTransactionsPage": defaultFiltersActiveExecutions,
  "filters/StatementsPage": defaultFilters,
  "filters/TransactionsPage": defaultFilters,
  [LocalStorageKeys.DB_FILTERS]: defaultFilters,
  "filters/SessionsPage": defaultFiltersForSessionsPage,
  "filters/InsightsPage": defaultFiltersInsights,
  "filters/SchemaInsightsPage": defaultFiltersSchemaInsights,
  [LocalStorageKeys.DB_DETAILS_TABLES_PAGE_FILTERS]: defaultFilters,
  "search/StatementsPage": null,
  "search/TransactionsPage": null,
  [LocalStorageKeys.DB_SEARCH]: null,
  "typeSetting/JobsPage": defaultJobTypeSetting,
  [LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SEARCH]: null,
  "statusSetting/JobsPage": defaultJobStatusSetting,
  [LocalStorageKeys.ACTIVE_EXECUTIONS_IS_AUTOREFRESH_ENABLED]:
    defaultIsAutoRefreshEnabledSetting,
  "requestTime/StatementsPage": null,
  "requestTime/TransactionsPage": null,
};

const localStorageSlice = createSlice({
  name: `${DOMAIN_NAME}/localStorage`,
  initialState,
  reducers: {
    update: (state: any, action: PayloadAction<Payload>) => {
      state[action.payload.key] = action.payload.value;
    },
    updateTimeScale: (
      state,
      action: PayloadAction<TypedPayload<TimeScale>>,
    ) => {
      state[LocalStorageKeys.GLOBAL_TIME_SCALE] = action.payload.value;
    },
  },
});

export const { actions, reducer } = localStorageSlice;

export const updateStmtsPageLimitAction = (
  limit: number,
): PayloadAction<Payload> =>
  localStorageSlice.actions.update({
    key: LocalStorageKeys.STMT_FINGERPRINTS_LIMIT,
    value: limit,
  });

export const updateStmsPageReqSortAction = (
  sort: SqlStatsSortType,
): PayloadAction<Payload> =>
  localStorageSlice.actions.update({
    key: LocalStorageKeys.STMT_FINGERPRINTS_SORT,
    value: sort,
  });

export const updateTxnsPageLimitAction = (
  limit: number,
): PayloadAction<Payload> =>
  localStorageSlice.actions.update({
    key: LocalStorageKeys.TXN_FINGERPRINTS_LIMIT,
    value: limit,
  });

export const updateTxnsPageReqSortAction = (
  sort: SqlStatsSortType,
): PayloadAction<Payload> =>
  localStorageSlice.actions.update({
    key: LocalStorageKeys.TXN_FINGERPRINTS_SORT,
    value: sort,
  });
