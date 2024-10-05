// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";
import { adminUISelector, localStorageSelector } from "../utils/selectors";
import { LocalStorageKeys } from "../localStorage";
import { SortSetting } from "../../sortedtable";
import { AppState } from "../reducers";
import { Filters } from "../../queryFilter";

export const databasesListSelector = createSelector(
  adminUISelector,
  adminUiState => adminUiState?.databasesList,
);

export const selectDatabasesSortSetting = (state: AppState): SortSetting => {
  return localStorageSelector(state)[LocalStorageKeys.DB_SORT];
};

export const selectDatabasesFilters = (state: AppState): Filters => {
  return localStorageSelector(state)[LocalStorageKeys.DB_FILTERS];
};

export const selectDatabasesSearch = (state: AppState): string => {
  return localStorageSelector(state)[LocalStorageKeys.DB_SEARCH];
};
