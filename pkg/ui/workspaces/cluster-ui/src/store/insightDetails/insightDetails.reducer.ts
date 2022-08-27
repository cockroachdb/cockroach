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
import { DOMAIN_NAME } from "../utils";
import moment, { Moment } from "moment";
import {
  TransactionInsightEventDetailsRequest,
  TransactionInsightEventDetailsResponse,
} from "src/api/insightsApi";

export type InsightDetailsState = {
  data: TransactionInsightEventDetailsResponse | null;
  lastUpdated: Moment | null;
  lastError: Error;
  valid: boolean;
};

const initialState: InsightDetailsState = {
  data: null,
  lastUpdated: null,
  lastError: null,
  valid: true,
};

const insightDetailsSlice = createSlice({
  name: `${DOMAIN_NAME}/insightDetailsSlice`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<TransactionInsightEventDetailsResponse>,
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
    refresh: (
      _,
      action: PayloadAction<TransactionInsightEventDetailsRequest>,
    ) => {},
    request: (
      _,
      action: PayloadAction<TransactionInsightEventDetailsRequest>,
    ) => {},
  },
});

export const { reducer, actions } = insightDetailsSlice;
