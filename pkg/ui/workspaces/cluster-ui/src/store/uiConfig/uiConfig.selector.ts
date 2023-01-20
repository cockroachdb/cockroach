// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { AppState } from "../reducers";

export const selectUIConfig = createSelector(
  (state: AppState) => state.adminUI?.uiConfig,
  uiConfig => uiConfig,
);

export const selectIsTenant = createSelector(
  selectUIConfig,
  uiConfig => uiConfig.isTenant,
);

export const selectHasViewActivityRedactedRole = createSelector(
  selectUIConfig,
  uiConfig => uiConfig.userSQLRoles.includes("VIEWACTIVITYREDACTED"),
);

export const selectHasAdminRole = createSelector(selectUIConfig, uiConfig =>
  uiConfig.userSQLRoles.includes("ADMIN"),
);
