// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "@reduxjs/toolkit";
import { AppState } from "../reducers";

const livenessesSelector = (state: AppState) => state.adminUI.liveness.data;

export const livenessStatusByNodeIDSelector = createSelector(
  livenessesSelector,
  livenesses => livenesses?.statuses || {},
);
