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
import { DOMAIN_NAME } from "../utils";
import { StatementsRequest } from "src/api/statementsApi";
import { TimeScale } from "../../timeScaleDropdown";
import moment from "moment";

export type StatementsResponse = cockroach.server.serverpb.StatementsResponse;

export type SQLStatsState = {
  data: StatementsResponse;
  lastError: Error;
  valid: boolean;
  lastUpdated: moment.Moment | null;
  inFlight: boolean;
};

const initialState: SQLStatsState = {
  data: null,
  lastError: null,
  valid: true,
  lastUpdated: null,
  inFlight: false,
};

export type UpdateTimeScalePayload = {
  ts: TimeScale;
};

const sqlStatsSlice = createSlice({
  name: `${DOMAIN_NAME}/sqlstats`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<StatementsResponse>) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
      state.lastUpdated = moment.utc();
      state.inFlight = false;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
      state.lastUpdated = moment.utc();
      state.inFlight = false;
    },
    invalidated: state => {
      state.valid = false;
    },
    refresh: (state, _action: PayloadAction<StatementsRequest>) => {
      state.inFlight = true;
    },
    request: (state, _action: PayloadAction<StatementsRequest>) => {
      state.inFlight = true;
    },
    updateTimeScale: (_, _action: PayloadAction<UpdateTimeScalePayload>) => {},
    reset: () => {},
  },
});

export const { reducer, actions } = sqlStatsSlice;
