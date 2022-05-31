// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { DOMAIN_NAME } from "../utils";
import { defaultFilters, Filters } from "../../queryFilter";
import { TimeScale, defaultTimeScaleSelected } from "../../timeScaleDropdown";

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
  "timeScale/SQLActivity": TimeScale;
  "sortSetting/ActiveStatementsPage": SortSetting;
  "sortSetting/ActiveTransactionsPage": SortSetting;
  "sortSetting/StatementsPage": SortSetting;
  "sortSetting/TransactionsPage": SortSetting;
  "sortSetting/SessionsPage": SortSetting;
  "filters/ActiveStatementsPage": Filters;
  "filters/ActiveTransactionsPage": Filters;
  "filters/StatementsPage": Filters;
  "filters/TransactionsPage": Filters;
  "filters/SessionsPage": Filters;
  "search/StatementsPage": string;
  "search/TransactionsPage": string;
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

const defaultFiltersActiveExecutions = {
  app: defaultFilters.app,
};

const defaultSessionsSortSetting: SortSetting = {
  ascending: false,
  columnTitle: "statementAge",
};

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
  "timeScale/SQLActivity":
    JSON.parse(localStorage.getItem("timeScale/SQLActivity")) ||
    defaultTimeScaleSelected,
  "sortSetting/ActiveStatementsPage":
    JSON.parse(localStorage.getItem("sortSetting/ActiveStatementsPage")) ||
    defaultSortSettingActiveExecutions,
  "sortSetting/ActiveTransactionsPage":
    JSON.parse(localStorage.getItem("sortSetting/ActiveTransactionsPage")) ||
    defaultSortSettingActiveExecutions,
  "sortSetting/StatementsPage":
    JSON.parse(localStorage.getItem("sortSetting/StatementsPage")) ||
    defaultSortSetting,
  "sortSetting/TransactionsPage":
    JSON.parse(localStorage.getItem("sortSetting/TransactionsPage")) ||
    defaultSortSetting,
  "sortSetting/SessionsPage":
    JSON.parse(localStorage.getItem("sortSetting/SessionsPage")) ||
    defaultSessionsSortSetting,
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
  "filters/SessionsPage":
    JSON.parse(localStorage.getItem("filters/SessionsPage")) || defaultFilters,
  "search/StatementsPage":
    JSON.parse(localStorage.getItem("search/StatementsPage")) || null,
  "search/TransactionsPage":
    JSON.parse(localStorage.getItem("search/TransactionsPage")) || null,
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
