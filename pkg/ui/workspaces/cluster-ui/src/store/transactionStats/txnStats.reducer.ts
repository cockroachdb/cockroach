// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { DOMAIN_NAME } from "../utils";
import { StatementsRequest } from "src/api/statementsApi";
import { TimeScale } from "../../timeScaleDropdown";
import moment from "moment-timezone";
import { StatementsResponse } from "../sqlStats";

export type TxnStatsState = {
  // Note that we request transactions from the
  // statements api, hence the StatementsResponse type here.
  data: StatementsResponse;
  inFlight: boolean;
  lastError: Error;
  valid: boolean;
  lastUpdated: moment.Moment | null;
};

const initialState: TxnStatsState = {
  data: null,
  inFlight: false,
  lastError: null,
  valid: false,
  lastUpdated: null,
};

const txnStatsSlice = createSlice({
  name: `${DOMAIN_NAME}/txnStats`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<StatementsResponse>) => {
      state.inFlight = false;
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
      state.lastUpdated = moment.utc();
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.inFlight = false;
      state.valid = false;
      state.lastError = action.payload;
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
