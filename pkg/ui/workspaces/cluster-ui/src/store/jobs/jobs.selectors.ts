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

export const selectSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/JobsPage"],
);

export const selectShowSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["showSetting/JobsPage"],
);

export const selectTypeSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["typeSetting/JobsPage"],
);

export const selectStatusSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["statusSetting/JobsPage"],
);

export const selectColumns = createSelector(
  localStorageSelector,
  // return array of columns if user have customized it or `null` otherwise
  localStorage =>
    localStorage["showColumns/JobsPage"]
      ? localStorage["showColumns/JobsPage"]?.split(",")
      : null,
);
