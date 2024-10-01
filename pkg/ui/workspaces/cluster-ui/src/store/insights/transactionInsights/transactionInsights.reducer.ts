// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import moment, { Moment } from "moment-timezone";

import { SqlApiResponse, TxnInsightsRequest } from "src/api";
import { TxnInsightEvent } from "src/insights";

import { DOMAIN_NAME } from "../../utils";

export type TxnInsightsState = {
  data: SqlApiResponse<TxnInsightEvent[]>;
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
  lastUpdated: Moment | null;
};

const initialState: TxnInsightsState = {
  data: null,
  lastError: null,
  valid: false,
  inFlight: false,
  lastUpdated: null,
};

const txnInsightsSlice = createSlice({
  name: `${DOMAIN_NAME}/txnInsightsSlice`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<SqlApiResponse<TxnInsightEvent[]>>,
    ) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
      state.inFlight = false;
      state.lastUpdated = moment.utc();
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
      state.inFlight = false;
    },
    invalidated: state => {
      state.valid = false;
    },
    refresh: (state, _action: PayloadAction<TxnInsightsRequest>) => {
      state.inFlight = true;
    },
    request: (state, _action: PayloadAction<TxnInsightsRequest>) => {
      state.inFlight = true;
    },
  },
});

export const { reducer, actions } = txnInsightsSlice;
