// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import moment, { Moment } from "moment-timezone";

import { SqlApiResponse, ErrorWithKey, StmtInsightsReq } from "src/api";

import { StmtInsightEvent } from "../../../insights";
import { DOMAIN_NAME } from "../../utils";

export type StatementFingerprintInsightsState = {
  data: SqlApiResponse<StmtInsightEvent[]> | null;
  lastUpdated: Moment | null;
  lastError: Error;
  valid: boolean;
};

export type StatementFingerprintInsightsCachedState = {
  cachedData: { [id: string]: StatementFingerprintInsightsState };
};

export type FingerprintInsightResponseWithKey = {
  response: SqlApiResponse<StmtInsightEvent[]>;
  key: string;
};

const initialState: StatementFingerprintInsightsCachedState = {
  cachedData: {},
};

const statementFingerprintInsightsSlice = createSlice({
  name: `${DOMAIN_NAME}/statementFingerprintInsightsSlice`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<FingerprintInsightResponseWithKey>,
    ) => {
      state.cachedData[action.payload.key] = {
        data: action.payload.response,
        valid: true,
        lastError: null,
        lastUpdated: moment.utc(),
      };
    },
    failed: (state, action: PayloadAction<ErrorWithKey>) => {
      state.cachedData[action.payload.key] = {
        data: null,
        valid: false,
        lastError: action.payload.err,
        lastUpdated: null,
      };
    },
    invalidated: (state, action: PayloadAction<{ key: string }>) => {
      delete state.cachedData[action.payload.key];
    },
    refresh: (_, _action: PayloadAction<StmtInsightsReq>) => {},
    request: (_, _action: PayloadAction<StmtInsightsReq>) => {},
  },
});

export const { reducer, actions } = statementFingerprintInsightsSlice;
