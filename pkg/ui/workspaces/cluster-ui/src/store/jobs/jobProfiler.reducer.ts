// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { DOMAIN_NAME, noopReducer } from "../utils";
import moment from "moment-timezone";
import { createInitialState, RequestState } from "src/api/types";
import {
  CollectExecutionDetailsRequest,
  ListJobProfilerExecutionDetailsRequest,
  ListJobProfilerExecutionDetailsResponse,
} from "src/api";

export type JobProfilerExecutionDetailFilesState =
  RequestState<ListJobProfilerExecutionDetailsResponse>;

export const initialState =
  createInitialState<ListJobProfilerExecutionDetailsResponse>();

const JobProfilerExecutionDetailsSlice = createSlice({
  name: `${DOMAIN_NAME}/jobProfilerExecutionDetails`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<ListJobProfilerExecutionDetailsResponse>,
    ) => {
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
    refresh: (
      state,
      _action: PayloadAction<ListJobProfilerExecutionDetailsRequest>,
    ) => {
      state.inFlight = true;
    },
    request: (
      state,
      _action: PayloadAction<ListJobProfilerExecutionDetailsRequest>,
    ) => {
      state.inFlight = true;
    },
    collectExecutionDetails: (
      _state,
      _action: PayloadAction<CollectExecutionDetailsRequest>,
    ) => {},
    collectExecutionDetailsCompleted: noopReducer,
    collectExecutionDetailsFailed: (
      _state,
      _action: PayloadAction<Error>,
    ) => {},
  },
});

export const { reducer, actions } = JobProfilerExecutionDetailsSlice;
