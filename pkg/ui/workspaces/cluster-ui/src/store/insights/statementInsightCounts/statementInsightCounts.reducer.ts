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
import moment, { Moment } from "moment";
import {
  StmtInsightsReq,
  StatementInsightCounts,
} from "src/api/stmtInsightsApi";

export type StatementInsightCountsState = {
  data: StatementInsightCounts;
  lastUpdated: Moment;
  lastError: Error;
  valid: boolean;
};

const initialState: StatementInsightCountsState = {
  data: null,
  lastUpdated: null,
  lastError: null,
  valid: true,
};

const statementInsightCountsSlice = createSlice({
  name: `${DOMAIN_NAME}/statementInsightCountsSlice`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<StatementInsightCounts>) => {
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
    refresh: (_, _action: PayloadAction<StmtInsightsReq>) => {},
    request: (_, _action: PayloadAction<StmtInsightsReq>) => {},
  },
});

export const { reducer, actions } = statementInsightCountsSlice;
