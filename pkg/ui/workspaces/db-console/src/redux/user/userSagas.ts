// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { AdminUIState } from "src/redux/state";

export const selectHasViewActivityRedactedRole = createSelector(
  (state: AdminUIState) => state.cachedData,
  cachedData =>
    cachedData.userSQLRoles.data?.roles?.includes("VIEWACTIVITYREDACTED"),
);

export const selectHasAdminRole = createSelector(
  (state: AdminUIState) => state.cachedData,
  cachedData => cachedData.userSQLRoles.data?.roles?.includes("ADMIN"),
);
