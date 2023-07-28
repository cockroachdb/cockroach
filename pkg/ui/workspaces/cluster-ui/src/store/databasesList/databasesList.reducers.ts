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
