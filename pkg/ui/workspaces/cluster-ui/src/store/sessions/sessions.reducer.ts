// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import moment, { Moment } from "moment-timezone";

import { SessionsRequest } from "src/api/sessionsApi";

import { DOMAIN_NAME } from "../utils";

type SessionsResponse = cockroach.server.serverpb.ListSessionsResponse;

export type SessionsState = {
  data: SessionsResponse;
  lastUpdated: Moment | null;
  lastError: Error;
  valid: boolean;
};

const initialState: SessionsState = {
  data: null,
  lastUpdated: null,
  lastError: null,
  valid: true,
};

const sessionsSlice = createSlice({
  name: `${DOMAIN_NAME}/sessions`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<SessionsResponse>) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
      state.lastUpdated = moment.utc();
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
    },
    invalidated: state => {
      state.valid = false;
    },
    // Define actions with optional payload using prepare callback
    refresh: {
      reducer: () => {},
      prepare: (payload?: SessionsRequest) => ({ payload }),
    },
    request: {
      reducer: () => {},
      prepare: (payload?: SessionsRequest) => ({ payload }),
    },
  },
});

export const { reducer, actions } = sessionsSlice;
