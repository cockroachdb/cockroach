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
import { DOMAIN_NAME } from "../../utils";
import { TxnInsightEvent } from "src/insights";
import { TxnInsightsRequest } from "src/api";

export type TxnInsightsState = {
  data: TxnInsightEvent[];
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
};

const initialState: TxnInsightsState = {
  data: null,
  lastError: null,
  valid: false,
  inFlight: false,
};

const txnInsightsSlice = createSlice({
  name: `${DOMAIN_NAME}/txnInsightsSlice`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<TxnInsightEvent[]>) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
      state.inFlight = false;
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
