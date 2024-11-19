// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { databasesListSelector } from "src/store/databasesList/databasesList.selectors";

import { localStorageSelector } from "../store/utils/selectors";

// selectDatabases returns the array of all databases in the cluster.
export const selectDatabases = createSelector(databasesListSelector, state => {
  if (!state?.data) {
    return [];
  }

  return state.data.databases
    .filter((dbName: string) => dbName !== null && dbName.length > 0)
    .sort();
});

export const selectColumns = createSelector(
  localStorageSelector,
  // return array of columns if user have customized it or `null` otherwise
  localStorage =>
    localStorage["showColumns/StatementsPage"]
      ? localStorage["showColumns/StatementsPage"]?.split(",")
      : null,
);

export const selectSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/StatementsPage"],
);

export const selectRequestTime = createSelector(
  localStorageSelector,
  localStorage => localStorage["requestTime/StatementsPage"],
);

export const selectFilters = createSelector(
  localStorageSelector,
  localStorage => localStorage["filters/StatementsPage"],
);

export const selectSearch = createSelector(
  localStorageSelector,
  localStorage => localStorage["search/StatementsPage"],
);
