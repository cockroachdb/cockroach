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
import { defaultFilters, Filters } from "../../queryFilter";
import { TimeScale, defaultTimeScaleSelected } from "../../timeScaleDropdown";
import {
  SqlStatsSortType,
  DEFAULT_STATS_REQ_OPTIONS,
} from "src/api/statementsApi";

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
}

export type LocalStorageState = {
  "adminUi/showDiagnosticsModal": boolean;
  "showColumns/StatementsPage": string;
  "showColumns/TransactionPage": string;
  "showColumns/SessionsPage": string;
  [LocalStorageKeys.GLOBAL_TIME_SCALE]: TimeScale;
  [LocalStorageKeys.STMT_FINGERPRINTS_LIMIT]: number;
  [LocalStorageKeys.STMT_FINGERPRINTS_SORT]: SqlStatsSortType;
  [LocalStorageKeys.TXN_FINGERPRINTS_LIMIT]: number;
  [LocalStorageKeys.TXN_FINGERPRINTS_SORT]: SqlStatsSortType;
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
  value: unknown;
};

export type TypedPayload<T> = {
  value: T;
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
  [LocalStorageKeys.STMT_FINGERPRINTS_LIMIT]:
    JSON.parse(
      localStorage.getItem(LocalStorageKeys.STMT_FINGERPRINTS_LIMIT),
    ) || DEFAULT_STATS_REQ_OPTIONS.limit,
  [LocalStorageKeys.STMT_FINGERPRINTS_SORT]:
    JSON.parse(localStorage.getItem(LocalStorageKeys.STMT_FINGERPRINTS_SORT)) ||
    DEFAULT_STATS_REQ_OPTIONS.sort,
  "showColumns/StatementsPage":
    JSON.parse(localStorage.getItem("showColumns/StatementsPage")) || null,
  "showColumns/TransactionPage":
    JSON.parse(localStorage.getItem("showColumns/TransactionPage")) || null,
  [LocalStorageKeys.TXN_FINGERPRINTS_LIMIT]:
    JSON.parse(localStorage.getItem(LocalStorageKeys.TXN_FINGERPRINTS_LIMIT)) ||
    DEFAULT_STATS_REQ_OPTIONS.limit,
  [LocalStorageKeys.TXN_FINGERPRINTS_SORT]:
    JSON.parse(localStorage.getItem(LocalStorageKeys.TXN_FINGERPRINTS_SORT)) ||
    DEFAULT_STATS_REQ_OPTIONS.sort,
  "showColumns/SessionsPage":
    JSON.parse(localStorage.getItem("showColumns/SessionsPage")) || null,
  [LocalStorageKeys.GLOBAL_TIME_SCALE]:
    JSON.parse(localStorage.getItem(LocalStorageKeys.GLOBAL_TIME_SCALE)) ||
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
