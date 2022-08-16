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
import { SchedulesRequest, SchedulesResponse } from "src/api/schedulesApi";
import { DOMAIN_NAME } from "../utils";

export type SchedulesState = {
  data: SchedulesResponse;
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
};

const initialState: SchedulesState = {
  data: null,
  lastError: null,
  valid: true,
  inFlight: false,
};

const SchedulesSlice = createSlice({
  name: `${DOMAIN_NAME}/schedules`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<SchedulesResponse>) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
      state.inFlight = false;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
    },
    invalidated: state => {
      state.valid = false;
    },
    refresh: (_, action: PayloadAction<SchedulesRequest>) => {},
    request: (_, action: PayloadAction<SchedulesRequest>) => {},
    reset: (_, action: PayloadAction<SchedulesRequest>) => {},
    updateFilteredSchedules: (_, action: PayloadAction<SchedulesRequest>) => {},
  },
});

export const { reducer, actions } = SchedulesSlice;
