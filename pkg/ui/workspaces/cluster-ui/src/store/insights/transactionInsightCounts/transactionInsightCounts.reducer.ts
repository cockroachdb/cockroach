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
import moment, { Moment } from "moment";
import {
  TxnInsightsRequest,
  TransactionInsightCounts,
} from "src/api/txnInsightsApi";

export type TransactionInsightCountsState = {
  data: TransactionInsightCounts;
  lastUpdated: Moment;
  lastError: Error;
  valid: boolean;
};

const initialState: TransactionInsightCountsState = {
  data: null,
  lastUpdated: null,
  lastError: null,
  valid: true,
};

const transactionInsightCountsSlice = createSlice({
  name: `${DOMAIN_NAME}/transactionInsightCountsSlice`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<TransactionInsightCounts>) => {
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
    refresh: (_, _action: PayloadAction<TxnInsightsRequest>) => {},
    request: (_, _action: PayloadAction<TxnInsightsRequest>) => {},
  },
});

export const { reducer, actions } = transactionInsightCountsSlice;
