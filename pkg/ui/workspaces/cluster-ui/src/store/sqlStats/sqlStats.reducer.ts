// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import moment from "moment-timezone";

import { StatementsRequest } from "src/api/statementsApi";
import { createInitialState, RequestState } from "src/api/types";

import { TimeScale } from "../../timeScaleDropdown";
import { DOMAIN_NAME } from "../utils";

export type StatementsResponse = cockroach.server.serverpb.StatementsResponse;

export type SQLStatsState = RequestState<StatementsResponse>;

const initialState = createInitialState<StatementsResponse>();

export type UpdateTimeScalePayload = {
  ts: TimeScale;
};

// This is actually statements only, despite the SQLStatsState name.
// We can rename this in the future. Leaving it now to reduce backport surface area.
const sqlStatsSlice = createSlice({
  name: `${DOMAIN_NAME}/sqlstats`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<StatementsResponse>) => {
      state.inFlight = false;
      state.data = action.payload;
      state.valid = true;
      state.error = null;
      state.lastUpdated = moment.utc();
      state.inFlight = false;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.error = action.payload;
      state.lastUpdated = moment.utc();
      state.inFlight = false;
    },
    invalidated: state => {
      state.inFlight = false;
      state.valid = false;
    },
    refresh: (state, _action: PayloadAction<StatementsRequest>) => {
      state.inFlight = true;
    },
    request: (state, _action: PayloadAction<StatementsRequest>) => {
      state.inFlight = true;
    },
    updateTimeScale: (_, _action: PayloadAction<UpdateTimeScalePayload>) => {},
    reset: (_, _action: PayloadAction<StatementsRequest>) => {},
  },
});

export const { reducer, actions } = sqlStatsSlice;
