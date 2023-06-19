// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { InsertJobProfilerBundleRequest, JobProfilerBundleResponse } from "src/api/jobProfilerBundleApi";
import { DOMAIN_NAME, noopReducer } from "../utils";


export type JobProfilerBundleState = {
    data: JobProfilerBundleResponse;
    lastError: Error;
    valid: boolean;
};

const initialState: JobProfilerBundleState = {
    data: null,
    valid: true,
    lastError: null,
};

const JobProfilerBundlesSlice = createSlice({
    name: `${DOMAIN_NAME}/jobProfilerBundles`,
    initialState,
    reducers: {
        received: (
            state: JobProfilerBundleState,
            action: PayloadAction<JobProfilerBundleResponse>,
        ) => {
            state.data = action.payload;
            state.lastError = null;
            state.valid = true;
        },
        failed: (
            state: JobProfilerBundleState,
            action: PayloadAction<Error>,
        ) => {
            state.lastError = action.payload;
            state.valid = false;
        },
        refresh: noopReducer,
        request: noopReducer,
        invalidated: noopReducer,
        createBundle: (
            _state,
            _action: PayloadAction<InsertJobProfilerBundleRequest>,
        ) => { },
        createBundleCompleted: noopReducer,
        createBundleFailed: (_state, _action: PayloadAction<Error>) => { },
    },
});

export const { actions, reducer } = JobProfilerBundlesSlice;
