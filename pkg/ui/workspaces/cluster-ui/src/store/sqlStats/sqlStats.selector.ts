// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { adminUISelector } from "../utils/selectors";

export const sqlStatsSelector = createSelector(
  adminUISelector,
  adminUiState => adminUiState?.statements,
);
