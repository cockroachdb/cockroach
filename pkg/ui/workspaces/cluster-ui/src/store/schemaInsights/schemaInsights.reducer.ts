// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import moment, { Moment } from "moment-timezone";

import { SchemaInsightReqParams, SqlApiResponse } from "src/api";

import { InsightRecommendation } from "../../insights";
import { DOMAIN_NAME } from "../utils";

export type SchemaInsightsState = {
  data: SqlApiResponse<InsightRecommendation[]>;
  lastUpdated: Moment;
  lastError: Error;
  valid: boolean;
};

const initialState: SchemaInsightsState = {
  data: null,
  lastUpdated: null,
  lastError: null,
  valid: false,
};

const schemaInsightsSlice = createSlice({
  name: `${DOMAIN_NAME}/schemaInsightsSlice`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<SqlApiResponse<InsightRecommendation[]>>,
    ) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
      state.lastUpdated = moment.utc();
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
      state.lastUpdated = moment.utc();
    },
    invalidated: state => {
      state.valid = false;
      state.lastUpdated = moment.utc();
    },
    refresh: (_, _action: PayloadAction<SchemaInsightReqParams>) => {},
    request: (_, _action: PayloadAction<SchemaInsightReqParams>) => {},
  },
});

export const { reducer, actions } = schemaInsightsSlice;
