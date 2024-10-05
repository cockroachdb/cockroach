// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { AppState } from "../reducers";

export const selectUIConfig = createSelector(
  (state: AppState) => state.adminUI?.uiConfig,
  uiConfig => uiConfig,
);

export const selectIsTenant = createSelector(
  selectUIConfig,
  uiConfig => uiConfig?.isTenant,
);

export const selectHasViewActivityRedactedRole = createSelector(
  selectUIConfig,
  uiConfig => uiConfig?.userSQLRoles.includes("VIEWACTIVITYREDACTED"),
);

export const selectHasAdminRole = createSelector(selectUIConfig, uiConfig =>
  uiConfig?.userSQLRoles.includes("ADMIN"),
);
