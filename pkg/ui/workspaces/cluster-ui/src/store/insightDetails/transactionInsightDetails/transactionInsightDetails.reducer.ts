// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import moment, { Moment } from "moment-timezone";

import { SqlApiResponse, TxnInsightDetailsReqErrs } from "src/api";
import { ErrorWithKey } from "src/api/statementsApi";
import { TxnInsightDetails } from "src/insights";
import { DOMAIN_NAME } from "src/store/utils";

import {
  TxnInsightDetailsRequest,
  TxnInsightDetailsResponse,
} from "../../../api/txnInsightDetailsApi";

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
