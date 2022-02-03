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
  "showColumns/StatementsPage": string;
  "showColumns/TransactionPage": string;
  "showColumns/SessionsPage": string;
  "timeScale/SQLActivity": TimeScale;
  "sortSetting/StatementsPage": SortSetting;
  "sortSetting/TransactionsPage": SortSetting;
  "sortSetting/SessionsPage": SortSetting;
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

const defaultSessionsSortSetting: SortSetting = {
  ascending: false,
  columnTitle: "statementAge",
};

// TODO (koorosh): initial state should be restored from preserved keys in LocalStorage
const initialState: LocalStorageState = {
  "adminUi/showDiagnosticsModal":
    Boolean(JSON.parse(localStorage.getItem("adminUi/showDiagnosticsModal"))) ||
    false,
  "showColumns/StatementsPage":
    JSON.parse(localStorage.getItem("showColumns/StatementsPage")) || null,
  "showColumns/TransactionPage":
    JSON.parse(localStorage.getItem("showColumns/TransactionPage")) || null,
  "showColumns/SessionsPage":
    JSON.parse(localStorage.getItem("showColumns/SessionsPage")) || null,
  "timeScale/SQLActivity":
    JSON.parse(localStorage.getItem("timeScale/SQLActivity")) ||
    defaultTimeScaleSelected,
  "sortSetting/StatementsPage":
    JSON.parse(localStorage.getItem("sortSetting/StatementsPage")) ||
    defaultSortSetting,
  "sortSetting/TransactionsPage":
    JSON.parse(localStorage.getItem("sortSetting/TransactionsPage")) ||
    defaultSortSetting,
  "sortSetting/SessionsPage":
    JSON.parse(localStorage.getItem("sortSetting/SessionsPage")) ||
    defaultSessionsSortSetting,
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
