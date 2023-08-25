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
import moment, { Moment } from "moment-timezone";
import { InsightRecommendation } from "../../insights";
import { SchemaInsightReqParams, SqlApiResponse } from "src/api";

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
