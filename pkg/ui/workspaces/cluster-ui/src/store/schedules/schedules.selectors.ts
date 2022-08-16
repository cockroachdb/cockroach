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
import { localStorageSelector } from "../utils/selectors";
import { adminUISelector } from "../utils/selectors";

export const selectSchedulesState = createSelector(
  adminUISelector,
  adminUiState => adminUiState.schedules,
);

export const selectSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/SchedulesPage"],
);

export const selectShowSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["showSetting/SchedulesPage"],
);

export const selectStatusSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["statusSetting/SchedulesPage"],
);
