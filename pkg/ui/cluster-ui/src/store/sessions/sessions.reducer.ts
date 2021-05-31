// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { DOMAIN_NAME, noopReducer } from "../utils";

type SessionsResponse = cockroach.server.serverpb.ListSessionsResponse;

export type SessionsState = {
  data: SessionsResponse;
  lastError: Error;
  valid: boolean;
};

const initialState: SessionsState = {
  data: null,
  lastError: null,
  valid: true,
};

const ssessionsSlice = createSlice({
  name: `${DOMAIN_NAME}/sessions`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<SessionsResponse>) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
    },
    invalidated: state => {
      state.valid = false;
    },
    // Define actions that don't change state
    refresh: noopReducer,
    request: noopReducer,
  },
});

export const { reducer, actions } = ssessionsSlice;
