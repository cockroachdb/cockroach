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
import { ScheduleRequest, ScheduleResponse } from "src/api/schedulesApi";
import { DOMAIN_NAME } from "../utils";

export type ScheduleState = {
  data: ScheduleResponse;
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
};

const initialState: ScheduleState = {
  data: null,
  lastError: null,
  valid: true,
  inFlight: false,
};

const ScheduleSlice = createSlice({
  name: `${DOMAIN_NAME}/schedule`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<ScheduleResponse>) => {
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
    refresh: (_, action: PayloadAction<ScheduleRequest>) => {},
    request: (_, action: PayloadAction<ScheduleRequest>) => {},
    reset: (_, action: PayloadAction<ScheduleRequest>) => {},
  },
});

export const { reducer, actions } = ScheduleSlice;
