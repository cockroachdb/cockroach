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
import {
  LocalStorageKeys,
  LocalStorageState,
} from "src/store/localStorage/localStorage.reducer";
import { AppState } from "../reducers";

export const adminUISelector = createSelector(
  (state: AppState) => state.adminUI,
  adminUiState => adminUiState,
);

export const localStorageSelector = createSelector(
  adminUISelector,
  adminUiState => {
    if (adminUiState) {
      return adminUiState?.localStorage;
    }
    return {} as LocalStorageState;
  },
);

export const selectTimeScale = createSelector(
  localStorageSelector,
  localStorage => localStorage[LocalStorageKeys.GLOBAL_TIME_SCALE],
);

export const selectStmtsPageLimit = createSelector(
  localStorageSelector,
  localStorage => localStorage[LocalStorageKeys.STMT_FINGERPRINTS_LIMIT],
);

export const selectStmtsPageReqSort = createSelector(
  localStorageSelector,
  localStorage => localStorage[LocalStorageKeys.STMT_FINGERPRINTS_SORT],
);

export const selectTxnsPageLimit = createSelector(
  localStorageSelector,
  localStorage => localStorage[LocalStorageKeys.TXN_FINGERPRINTS_LIMIT],
);

export const selectTxnsPageReqSort = createSelector(
  localStorageSelector,
  localStorage => localStorage[LocalStorageKeys.TXN_FINGERPRINTS_SORT],
);
