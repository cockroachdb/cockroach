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
import moment from "moment-timezone";

import { JobsRequest, JobsResponse } from "src/api/jobsApi";
import { createInitialState, RequestState } from "src/api/types";

import { DOMAIN_NAME } from "../utils";

export type JobsState = RequestState<JobsResponse>;

export const initialState = createInitialState<JobsResponse>();

const JobsSlice = createSlice({
  name: `${DOMAIN_NAME}/jobs`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<JobsResponse>) => {
      state.data = action.payload;
      state.valid = true;
      state.error = null;
      state.inFlight = false;
      state.lastUpdated = moment.utc();
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.inFlight = false;
      state.valid = false;
      state.error = action.payload;
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
