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
import { StmtInsightEvent } from "src/insights";
import { SqlApiResponse, StmtInsightsReq } from "src/api";
import moment from "moment-timezone";

export type StmtInsightsState = {
  data: SqlApiResponse<StmtInsightEvent[]>;
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
  lastUpdated: moment.Moment | null;
};

const initialState: StmtInsightsState = {
  data: null,
  lastError: null,
  valid: false,
  inFlight: false,
  lastUpdated: null,
};

const statementInsightsSlice = createSlice({
  name: `${DOMAIN_NAME}/statementInsightsSlice`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<SqlApiResponse<StmtInsightEvent[]>>,
    ) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
      state.inFlight = false;
      state.lastUpdated = moment.utc();
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
      state.inFlight = false;
    },
    invalidated: state => {
      state.valid = false;
    },
    refresh: (state, _action: PayloadAction<StmtInsightsReq>) => {
      state.inFlight = true;
    },
    request: (state, _action: PayloadAction<StmtInsightsReq>) => {
      state.inFlight = true;
    },
  },
});

export const { reducer, actions } = statementInsightsSlice;
