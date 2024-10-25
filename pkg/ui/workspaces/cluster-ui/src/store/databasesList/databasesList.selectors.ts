// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { adminUISelector } from "../utils/selectors";

export const databasesListSelector = createSelector(
  adminUISelector,
  adminUiState => adminUiState?.databasesList,
);
