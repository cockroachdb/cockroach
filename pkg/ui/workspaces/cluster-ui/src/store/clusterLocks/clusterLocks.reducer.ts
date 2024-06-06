// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";

import { ClusterLocksResponse, SqlApiResponse } from "src/api";

import { DOMAIN_NAME, noopReducer } from "../utils";

export type ClusterLocksReqState = {
  data: SqlApiResponse<ClusterLocksResponse>;
  lastError: Error;
  valid: boolean;
};

const initialState: ClusterLocksReqState = {
  data: null,
  lastError: null,
  valid: true,
};

const clusterLocksSlice = createSlice({
  name: `${DOMAIN_NAME}/clusterLocks`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<SqlApiResponse<ClusterLocksResponse>>,
    ) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
    },
    // Define actions that don't change state.
    refresh: noopReducer,
    request: noopReducer,
  },
});

export const { reducer, actions } = clusterLocksSlice;
