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
import { DOMAIN_NAME, noopReducer } from "src/store/utils";
import moment, { Moment } from "moment";
import { TransactionInsightEventsResponse } from "src/api/insightsApi";

export type TransactionInsightsState = {
  data: TransactionInsightEventsResponse;
  lastUpdated: Moment;
  lastError: Error;
  valid: boolean;
};

const initialState: TransactionInsightsState = {
  data: null,
  lastUpdated: null,
  lastError: null,
  valid: true,
};

const transactionInsightsSlice = createSlice({
  name: `${DOMAIN_NAME}/transactionInsightsSlice`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<TransactionInsightEventsResponse>,
    ) => {
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
    refresh: noopReducer,
    request: noopReducer,
  },
});

export const { reducer, actions } = transactionInsightsSlice;
