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
import { DOMAIN_NAME } from "src/store/utils";
import moment, { Moment } from "moment";
import { ErrorWithKey } from "src/api/statementsApi";
import { TxnContentionInsightDetailsRequest } from "src/api/txnInsightsApi";
import { TxnContentionInsightDetails } from "src/insights";

export type TransactionInsightDetailsState = {
  data: TxnContentionInsightDetails | null;
  lastUpdated: Moment | null;
  lastError: Error;
  valid: boolean;
};

const txnInitialState: TransactionInsightDetailsState = {
  data: null,
  lastUpdated: null,
  lastError: null,
  valid: true,
};

export type TransactionInsightDetailsCachedState = {
  cachedData: {
    [id: string]: TransactionInsightDetailsState;
  };
};

const initialState: TransactionInsightDetailsCachedState = {
  cachedData: {},
};

const transactionInsightDetailsSlice = createSlice({
  name: `${DOMAIN_NAME}/transactionInsightDetailsSlice`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<TxnContentionInsightDetails>) => {
      if (action?.payload?.transactionExecutionID) {
        state.cachedData[action.payload.transactionExecutionID] = {
          data: action.payload,
          valid: true,
          lastError: null,
          lastUpdated: moment.utc(),
        };
      }
    },
    failed: (state, action: PayloadAction<ErrorWithKey>) => {
      state.cachedData[action.payload.key] = {
        data: null,
        valid: false,
        lastError: action?.payload?.err,
        lastUpdated: null,
      };
    },
    invalidated: (state, action: PayloadAction<{ key: string }>) => {
      delete state.cachedData[action.payload.key];
    },
    refresh: (
      _,
      _action: PayloadAction<TxnContentionInsightDetailsRequest>,
    ) => {},
    request: (
      _,
      _action: PayloadAction<TxnContentionInsightDetailsRequest>,
    ) => {},
  },
});

export const { reducer, actions } = transactionInsightDetailsSlice;
