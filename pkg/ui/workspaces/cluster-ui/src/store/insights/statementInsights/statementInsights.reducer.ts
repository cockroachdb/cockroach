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
import { StmtInsightsReq } from "src/api";
import { UpdateTimeScalePayload } from "../../sqlStats";

export type StmtInsightsState = {
  data: StmtInsightEvent[];
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
};

const initialState: StmtInsightsState = {
  data: null,
  lastError: null,
  valid: false,
  inFlight: false,
};

const statementInsightsSlice = createSlice({
  name: `${DOMAIN_NAME}/statementInsightsSlice`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<StmtInsightEvent[]>) => {
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
    refresh: (_, _action: PayloadAction<StmtInsightsReq>) => {},
    request: (_, _action: PayloadAction<StmtInsightsReq>) => {},
    updateTimeScale: (
      state,
      _action: PayloadAction<UpdateTimeScalePayload>,
    ) => {
      state.inFlight = true;
    },
  },
});

export const { reducer, actions } = statementInsightsSlice;
