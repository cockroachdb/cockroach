// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AppState } from "src";

import { LocalStorageKeys } from "src/store/localStorage";

import { localStorageSelector } from "../store/utils/selectors";

// selectIsAutoRefreshEnabled is still used by the active transactions
// page selectors. It will be removed when that page is migrated to SWR.
export const selectIsAutoRefreshEnabled = (state: AppState): boolean => {
  return localStorageSelector(state)[
    LocalStorageKeys.ACTIVE_EXECUTIONS_IS_AUTOREFRESH_ENABLED
  ];
};
