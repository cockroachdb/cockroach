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
import { DOMAIN_NAME, noopReducer } from "../utils";
import moment, { Moment } from "moment";
import { TransactionContentionEventsResponse } from "src/api/contentionApi";

export type TransactionContentionState = {
  data: TransactionContentionEventsResponse;
  lastUpdated: Moment | null;
  lastError: Error;
  valid: boolean;
};

const initialState: TransactionContentionState = {
  data: null,
  lastUpdated: null,
  lastError: null,
  valid: true,
};

const contentionTransactionsSlice = createSlice({
  name: `${DOMAIN_NAME}/contentionTransactions`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<TransactionContentionEventsResponse>,
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
    // Define actions that don't change state
    refresh: noopReducer,
    request: noopReducer,
  },
});

export const { reducer, actions } = contentionTransactionsSlice;
