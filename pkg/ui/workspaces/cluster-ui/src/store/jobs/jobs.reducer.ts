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
import { JobsRequest, JobsResponse } from "src/api/jobsApi";
import { DOMAIN_NAME } from "../utils";
import moment from "moment-timezone";

export type JobsState = {
  data: JobsResponse;
  lastError: Error;
  lastUpdated: moment.Moment;
  valid: boolean;
  inFlight: boolean;
};

const initialState: JobsState = {
  data: null,
  lastError: null,
  valid: true,
  inFlight: false,
  lastUpdated: null,
};

const JobsSlice = createSlice({
  name: `${DOMAIN_NAME}/jobs`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<JobsResponse>) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
      state.inFlight = false;
      state.lastUpdated = moment.utc();
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.inFlight = false;
      state.valid = false;
      state.lastError = action.payload;
      state.lastUpdated = moment.utc();
    },
    invalidated: state => {
      state.valid = false;
    },
    refresh: (state, _action: PayloadAction<JobsRequest>) => {
      state.inFlight = true;
    },
    request: (state, _action: PayloadAction<JobsRequest>) => {
      state.inFlight = true;
    },
  },
});

export const { reducer, actions } = JobsSlice;
