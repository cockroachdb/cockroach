// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { adminUISelector, localStorageSelector } from "../utils/selectors";

export const databasesListSelector = createSelector(
  adminUISelector,
  adminUiState => adminUiState.databasesList,
);

export const selectDatabasesSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/DatabasesPage"],
);

export const selectDatabasesFilters = createSelector(
  localStorageSelector,
  localStorage => localStorage["filters/DatabasesPage"],
);

export const selectDatabasesSearch = createSelector(
  localStorageSelector,
  localStorage => localStorage["search/DatabasesPage"],
);
