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
import { RequestState } from "../../api";
import moment from "moment-timezone";

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
