// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";

import { DatabasesListResponse } from "src/api";

import { DOMAIN_NAME, noopReducer } from "../utils";

export type DatabasesListState = {
  data: DatabasesListResponse;
  // Captures thrown errors.
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
};

const initialState: DatabasesListState = {
  data: null,
  lastError: undefined,
  valid: false,
  inFlight: false,
};

const databasesListSlice = createSlice({
  name: `${DOMAIN_NAME}/databasesList`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<DatabasesListResponse>) => {
      state.data = action.payload;
      state.valid = true;
      state.inFlight = false;
      state.lastError = null;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.data = null;
      state.valid = false;
      state.inFlight = false;
      state.lastError = action.payload;
    },
    request: (state, _: PayloadAction<void>) => {
      state.data = null;
      state.valid = false;
      state.inFlight = true;
    },
    refresh: noopReducer,
  },
});

export const { reducer, actions } = databasesListSlice;
