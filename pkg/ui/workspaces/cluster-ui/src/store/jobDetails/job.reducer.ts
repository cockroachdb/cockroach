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
import { JobRequest, JobResponse } from "src/api/jobsApi";
import { DOMAIN_NAME } from "../utils";

export type JobState = {
  data: JobResponse;
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
};

const initialState: JobState = {
  data: null,
  lastError: null,
  valid: true,
  inFlight: false,
};

const JobSlice = createSlice({
  name: `${DOMAIN_NAME}/job`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<JobResponse>) => {
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
    refresh: (_, action: PayloadAction<JobRequest>) => {},
    request: (_, action: PayloadAction<JobRequest>) => {},
    reset: (_, action: PayloadAction<JobRequest>) => {},
  },
});

export const { reducer, actions } = JobSlice;
