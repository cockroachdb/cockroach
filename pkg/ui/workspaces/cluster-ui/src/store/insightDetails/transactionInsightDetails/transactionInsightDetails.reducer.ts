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
import moment, { Moment } from "moment-timezone";
import { ErrorWithKey } from "src/api/statementsApi";
import {
  TxnInsightDetailsRequest,
  TxnInsightDetailsResponse,
} from "src/api/txnInsightsApi";
import { TxnInsightDetails } from "src/insights";
import { SqlApiResponse, TxnInsightDetailsReqErrs } from "src/api";

export type TxnInsightDetailsState = {
  data: TxnInsightDetails | null;
  lastUpdated: Moment | null;
  errors: TxnInsightDetailsReqErrs | null;
  valid: boolean;
  maxSizeReached: boolean;
};

export type TxnInsightDetailsCachedState = {
  cachedData: { [id: string]: TxnInsightDetailsState };
};

const initialState: TxnInsightDetailsCachedState = {
  cachedData: {},
};

const transactionInsightDetailsSlice = createSlice({
  name: `${DOMAIN_NAME}/transactionInsightDetailsSlice`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<SqlApiResponse<TxnInsightDetailsResponse>>,
    ) => {
      state.cachedData[action.payload.results.txnExecutionID] = {
        data: action.payload.results.result,
        valid: true,
        errors: action.payload.results.errors,
        lastUpdated: moment.utc(),
        maxSizeReached: action.payload.maxSizeReached,
      };
    },
    failed: (state, action: PayloadAction<ErrorWithKey>) => {
      state.cachedData[action.payload.key] = {
        data: null,
        valid: false,
        errors: {
          txnDetailsErr: action.payload.err,
          contentionErr: action.payload.err,
          statementsErr: action.payload.err,
        },
        lastUpdated: null,
        maxSizeReached: false,
      };
    },
    invalidated: (state, action: PayloadAction<{ key: string }>) => {
      delete state.cachedData[action.payload.key];
    },
    refresh: (_, _action: PayloadAction<TxnInsightDetailsRequest>) => {},
    request: (_, _action: PayloadAction<TxnInsightDetailsRequest>) => {},
  },
});

export const { reducer, actions } = transactionInsightDetailsSlice;
