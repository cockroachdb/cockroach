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
import { DOMAIN_NAME } from "../utils";
import { StatementsRequest } from "src/api/statementsApi";
import { TimeScale } from "../../timeScaleDropdown";
import moment from "moment";
import { StatementsResponse } from "../sqlStats";

export type TxnStatsData = cockroach.server.serverpb.IStatementsResponse;

export type TxnStatsState = {
  data: TxnStatsData;
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

export type UpdateTimeScalePayload = {
  ts: TimeScale;
};

// This is actually statements only, despite the SQLStatsState name.
// We can rename this in the future. Leaving it now to reduce backport surface area.
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
