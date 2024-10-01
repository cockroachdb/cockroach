// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AdminUIState } from "src/redux/state";

export const connectivitySelector = (state: AdminUIState) =>
  state.cachedData.connectivity;
