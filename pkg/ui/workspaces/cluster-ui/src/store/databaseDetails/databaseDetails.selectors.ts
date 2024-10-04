// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { localStorageSelector } from "../utils/selectors";
import { LocalStorageKeys } from "../localStorage";
import { AppState } from "../reducers";

export const selectDatabaseDetailsViewModeSetting = (state: AppState) => {
  const localStorage = localStorageSelector(state);
  return localStorage[LocalStorageKeys.DB_DETAILS_VIEW_MODE];
};

export const selectDatabaseDetailsTablesSortSetting = (state: AppState) => {
  const localStorage = localStorageSelector(state);
  return localStorage[LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SORT];
};

export const selectDatabaseDetailsGrantsSortSetting = (state: AppState) => {
  const localStorage = localStorageSelector(state);
  return localStorage[LocalStorageKeys.DB_DETAILS_GRANTS_PAGE_SORT];
};

export const selectDatabaseDetailsTablesFiltersSetting = (state: AppState) => {
  const localStorage = localStorageSelector(state);
  return localStorage[LocalStorageKeys.DB_DETAILS_TABLES_PAGE_FILTERS];
};

export const selectDatabaseDetailsTablesSearchSetting = (state: AppState) => {
  const localStorage = localStorageSelector(state);
  return localStorage[LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SEARCH];
};
