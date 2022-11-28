// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { DOMAIN_NAME } from "../utils";
import { defaultFilters, Filters } from "src/queryFilter/";
import { TimeScale, defaultTimeScaleSelected } from "../../timeScaleDropdown";
import { WorkloadInsightEventFilters } from "src/insights";

type SortSetting = {
  ascending: boolean;
  columnTitle: string;
};

export type LocalStorageState = {
  "adminUi/showDiagnosticsModal": boolean;
  "showColumns/ActiveStatementsPage": string;
  "showColumns/ActiveTransactionsPage": string;
  "showColumns/StatementsPage": string;
  "showColumns/TransactionPage": string;
  "showColumns/SessionsPage": string;
  "showColumns/StatementInsightsPage": string;
  "showColumns/JobsPage": string;
  "timeScale/SQLActivity": TimeScale;
  "sortSetting/ActiveStatementsPage": SortSetting;
  "sortSetting/ActiveTransactionsPage": SortSetting;
  "sortSetting/StatementsPage": SortSetting;
  "sortSetting/TransactionsPage": SortSetting;
  "sortSetting/SessionsPage": SortSetting;
  "sortSetting/JobsPage": SortSetting;
  "sortSetting/InsightsPage": SortSetting;
  "sortSetting/SchemaInsightsPage": SortSetting;
  "filters/ActiveStatementsPage": Filters;
  "filters/ActiveTransactionsPage": Filters;
  "filters/StatementsPage": Filters;
  "filters/TransactionsPage": Filters;
  "filters/DatabasesPage": string;
  "filters/SessionsPage": Filters;
  "filters/InsightsPage": WorkloadInsightEventFilters;
  "filters/SchemaInsightsPage": Filters;
  "search/StatementsPage": string;
  "search/TransactionsPage": string;
  "search/DatabasesPage": string;
  "typeSetting/JobsPage": number;
  "statusSetting/JobsPage": string;
  "showSetting/JobsPage": string;
};

type Payload = {
  key: keyof LocalStorageState;
  value: any;
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

const defaultFiltersActiveExecutions = {
  app: "",
};

const defaultFiltersInsights = {
  app: "",
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

// TODO (koorosh): initial state should be restored from preserved keys in LocalStorage
const initialState: LocalStorageState = {
  "adminUi/showDiagnosticsModal":
    Boolean(JSON.parse(localStorage.getItem("adminUi/showDiagnosticsModal"))) ||
    false,
  "showColumns/ActiveStatementsPage":
    JSON.parse(localStorage.getItem("showColumns/ActiveStatementsPage")) ??
    null,
  "showColumns/ActiveTransactionsPage":
    JSON.parse(localStorage.getItem("showColumns/ActiveTransactionsPage")) ??
    null,
  "showColumns/StatementsPage":
    JSON.parse(localStorage.getItem("showColumns/StatementsPage")) || null,
  "showColumns/TransactionPage":
    JSON.parse(localStorage.getItem("showColumns/TransactionPage")) || null,
  "showColumns/SessionsPage":
    JSON.parse(localStorage.getItem("showColumns/SessionsPage")) || null,
  "showColumns/StatementInsightsPage":
    JSON.parse(localStorage.getItem("showColumns/StatementInsightsPage")) ||
    null,
  "showColumns/JobsPage":
    JSON.parse(localStorage.getItem("showColumns/JobsPage")) || null,
  "showSetting/JobsPage":
    JSON.parse(localStorage.getItem("showSetting/JobsPage")) ||
    defaultJobShowSetting,
  "timeScale/SQLActivity":
    JSON.parse(localStorage.getItem("timeScale/SQLActivity")) ||
    defaultTimeScaleSelected,
  "sortSetting/ActiveStatementsPage":
    JSON.parse(localStorage.getItem("sortSetting/ActiveStatementsPage")) ||
    defaultSortSettingActiveExecutions,
  "sortSetting/ActiveTransactionsPage":
    JSON.parse(localStorage.getItem("sortSetting/ActiveTransactionsPage")) ||
    defaultSortSettingActiveExecutions,
  "sortSetting/JobsPage":
    JSON.parse(localStorage.getItem("sortSetting/JobsPage")) ||
    defaultJobsSortSetting,
  "sortSetting/StatementsPage":
    JSON.parse(localStorage.getItem("sortSetting/StatementsPage")) ||
    defaultSortSetting,
  "sortSetting/TransactionsPage":
    JSON.parse(localStorage.getItem("sortSetting/TransactionsPage")) ||
    defaultSortSetting,
  "sortSetting/SessionsPage":
    JSON.parse(localStorage.getItem("sortSetting/SessionsPage")) ||
    defaultSessionsSortSetting,
  "sortSetting/InsightsPage":
    JSON.parse(localStorage.getItem("sortSetting/InsightsPage")) ||
    defaultSortSettingInsights,
  "sortSetting/SchemaInsightsPage":
    JSON.parse(localStorage.getItem("sortSetting/SchemaInsightsPage")) ||
    defaultSortSettingSchemaInsights,
  "filters/ActiveStatementsPage":
    JSON.parse(localStorage.getItem("filters/ActiveStatementsPage")) ||
    defaultFiltersActiveExecutions,
  "filters/ActiveTransactionsPage":
    JSON.parse(localStorage.getItem("filters/ActiveTransactionsPage")) ||
    defaultFiltersActiveExecutions,
  "filters/StatementsPage":
    JSON.parse(localStorage.getItem("filters/StatementsPage")) ||
    defaultFilters,
  "filters/TransactionsPage":
    JSON.parse(localStorage.getItem("filters/TransactionsPage")) ||
    defaultFilters,
  "filters/DatabasesPage":
    JSON.parse(localStorage.getItem("filters/DatabasessPage")) ||
    defaultFilters,
  "filters/SessionsPage":
    JSON.parse(localStorage.getItem("filters/SessionsPage")) || defaultFilters,
  "filters/InsightsPage":
    JSON.parse(localStorage.getItem("filters/InsightsPage")) ||
    defaultFiltersInsights,
  "filters/SchemaInsightsPage":
    JSON.parse(localStorage.getItem("filters/SchemaInsightsPage")) ||
    defaultFiltersSchemaInsights,
  "search/StatementsPage":
    JSON.parse(localStorage.getItem("search/StatementsPage")) || null,
  "search/TransactionsPage":
    JSON.parse(localStorage.getItem("search/TransactionsPage")) || null,
  "search/DatabasesPage":
    JSON.parse(localStorage.getItem("search/DatabasesPage")) || null,
  "typeSetting/JobsPage":
    JSON.parse(localStorage.getItem("typeSetting/JobsPage")) ||
    defaultJobTypeSetting,
  "statusSetting/JobsPage":
    JSON.parse(localStorage.getItem("statusSetting/JobsPage")) ||
    defaultJobStatusSetting,
};

const localStorageSlice = createSlice({
  name: `${DOMAIN_NAME}/localStorage`,
  initialState,
  reducers: {
    update: (state: any, action: PayloadAction<Payload>) => {
      state[action.payload.key] = action.payload.value;
    },
  },
});

export const { actions, reducer } = localStorageSlice;
