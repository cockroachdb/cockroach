// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
