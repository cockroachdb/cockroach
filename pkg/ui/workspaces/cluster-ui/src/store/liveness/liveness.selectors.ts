// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "@reduxjs/toolkit";

import { AppState } from "../reducers";

const livenessesSelector = (state: AppState) => state.adminUI?.liveness.data;

export const livenessStatusByNodeIDSelector = createSelector(
  livenessesSelector,
  livenesses => livenesses?.statuses || {},
);
