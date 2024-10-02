// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import moment from "moment-timezone";

import { StatementsRequest } from "src/api/statementsApi";
import { createInitialState, RequestState } from "src/api/types";

import { StatementsResponse } from "../sqlStats";
import { DOMAIN_NAME } from "../utils";

// Note that we request transactions from the
// statements api, hence the StatementsResponse type here.
export type TxnStatsState = RequestState<StatementsResponse>;

const initialState = createInitialState<StatementsResponse>();

const txnStatsSlice = createSlice({
  name: `${DOMAIN_NAME}/txnStats`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<StatementsResponse>) => {
      state.inFlight = false;
      state.data = action.payload;
      state.valid = true;
      state.error = null;
      state.lastUpdated = moment.utc();
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.inFlight = false;
      state.valid = false;
      state.error = action.payload;
      state.lastUpdated = moment.utc();
    },
    invalidated: state => {
      state.inFlight = false;
      state.valid = false;
    },
    refresh: (state, _: PayloadAction<StatementsRequest>) => {
      state.inFlight = true;
    },
    request: (state, _: PayloadAction<StatementsRequest>) => {
      state.inFlight = true;
    },
  },
});

export const { reducer, actions } = txnStatsSlice;
