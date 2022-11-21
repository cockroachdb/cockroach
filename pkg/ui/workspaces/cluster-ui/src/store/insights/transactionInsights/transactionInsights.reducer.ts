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
import { TxnContentionInsightEvent } from "src/insights";
import { ExecutionInsightsRequest } from "../../../api";
import { UpdateTimeScalePayload } from "../../sqlStats";

export type TransactionInsightsState = {
  data: TxnContentionInsightEvent[];
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
};

const initialState: TransactionInsightsState = {
  data: null,
  lastError: null,
  valid: false,
  inFlight: false,
};

const transactionInsightsSlice = createSlice({
  name: `${DOMAIN_NAME}/transactionInsightsSlice`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<TxnContentionInsightEvent[]>) => {
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
    refresh: (_, _action: PayloadAction<ExecutionInsightsRequest>) => {},
    request: (_, _action: PayloadAction<ExecutionInsightsRequest>) => {},
    updateTimeScale: (
      state,
      _action: PayloadAction<UpdateTimeScalePayload>,
    ) => {
      state.inFlight = true;
    },
  },
});

export const { reducer, actions } = transactionInsightsSlice;
