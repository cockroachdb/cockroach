// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { DOMAIN_NAME, noopReducer } from "../utils";
import { StatementsRequest } from "src/api/statementsApi";

type StatementsResponse = cockroach.server.serverpb.StatementsResponse;

export type StatementsState = {
  data: StatementsResponse;
  lastError: Error;
  valid: boolean;
};

const initialState: StatementsState = {
  data: null,
  lastError: null,
  valid: true,
};

export type UpdateDateRangePayload = {
  start: number;
  end: number;
};

const statementsSlice = createSlice({
  name: `${DOMAIN_NAME}/statements`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<StatementsResponse>) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
    },
    invalidated: state => {
      state.valid = false;
    },
    refresh: (_, action?: PayloadAction<StatementsRequest>) => {},
    request: (_, action?: PayloadAction<StatementsRequest>) => {},
    updateDateRange: (_, action: PayloadAction<UpdateDateRangePayload>) => {},
  },
});

export const { reducer, actions } = statementsSlice;
