// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { LocalStorageKeys } from "../localStorage";
import { SortSetting } from "../../sortedtable";
import { AppState } from "../reducers";
import { ViewMode } from "../../databaseDetailsPage";
import { Filters } from "../../queryFilter";
import { localStorageSelector } from "../utils/selectors";
import { KeyedDatabaseDetailsState } from "./databaseDetails.reducer";

export const selectDatabaseDetails = (
  state: AppState,
): KeyedDatabaseDetailsState => {
  return state.adminUI?.databaseDetails;
};

export const selectDatabaseDetailsViewModeSetting = (
  state: AppState,
): ViewMode => {
  return localStorageSelector(state)[LocalStorageKeys.DB_DETAILS_VIEW_MODE];
};

export const selectDatabaseDetailsTablesSortSetting = (
  state: AppState,
): SortSetting => {
  return localStorageSelector(state)[
    LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SORT
  ];
};

export const selectDatabaseDetailsGrantsSortSetting = (
  state: AppState,
): SortSetting => {
  return localStorageSelector(state)[
    LocalStorageKeys.DB_DETAILS_GRANTS_PAGE_SORT
  ];
};

export const selectDatabaseDetailsTablesFiltersSetting = (
  state: AppState,
): Filters => {
  return localStorageSelector(state)[
    LocalStorageKeys.DB_DETAILS_TABLES_PAGE_FILTERS
  ];
};

export const selectDatabaseDetailsTablesSearchSetting = (
  state: AppState,
): string => {
  return localStorageSelector(state)[
    LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SEARCH
  ];
};
