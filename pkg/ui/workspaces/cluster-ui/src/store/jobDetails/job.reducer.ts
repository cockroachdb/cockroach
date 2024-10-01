// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import moment from "moment-timezone";

import {
  ErrorWithKey,
  JobRequest,
  JobResponse,
  JobResponseWithKey,
} from "src/api/jobsApi";

import { RequestState } from "../../api";
import { DOMAIN_NAME } from "../utils";

export type JobState = RequestState<JobResponse>;

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
        error: null,
        inFlight: false,
        lastUpdated: moment.utc(),
      };
    },
    failed: (state, action: PayloadAction<ErrorWithKey>) => {
      state.cachedData[action.payload.key] = {
        data: null,
        valid: false,
        error: action.payload.err,
        inFlight: false,
        lastUpdated: moment.utc(),
      };
    },
    invalidated: (state, action: PayloadAction<{ key: string }>) => {
      const lastUpdated = state.cachedData[action.payload.key]?.lastUpdated;
      state.cachedData[action.payload.key] = {
        data: null,
        valid: false,
        error: null,
        inFlight: false,
        lastUpdated,
      };
    },
    refresh: (_, _action: PayloadAction<JobRequest>) => {},
    request: (_, _action: PayloadAction<JobRequest>) => {},
  },
});

export const { reducer, actions } = JobSlice;
