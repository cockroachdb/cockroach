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
import {
  ErrorWithKey,
  JobRequest,
  JobResponse,
  JobResponseWithKey,
} from "src/api/jobsApi";
import { DOMAIN_NAME } from "../utils";

export type JobState = {
  data: JobResponse;
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
};

export type JobDetailsReducerState = {
  cachedData: {
    [id: string]: JobState;
  };
};

const initialState: JobDetailsReducerState = {
  cachedData: {},
};

const JobSlice = createSlice({
  name: `${DOMAIN_NAME}/job`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<JobResponseWithKey>) => {
      state.cachedData[action.payload.key] = {
        data: action.payload.jobResponse,
        valid: true,
        lastError: null,
        inFlight: false,
      };
    },
    failed: (state, action: PayloadAction<ErrorWithKey>) => {
      state.cachedData[action.payload.key] = {
        data: null,
        valid: false,
        lastError: action.payload.err,
        inFlight: false,
      };
    },
    invalidated: (state, action: PayloadAction<{ key: string }>) => {
      state.cachedData[action.payload.key] = {
        data: null,
        valid: false,
        lastError: null,
        inFlight: false,
      };
    },
    refresh: (_, _action: PayloadAction<JobRequest>) => {},
    request: (_, _action: PayloadAction<JobRequest>) => {},
  },
});

export const { reducer, actions } = JobSlice;
