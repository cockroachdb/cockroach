// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";

import { ErrorWithKey } from "../../api";
import {
  TableIndexStatsRequest,
  TableIndexStatsResponse,
  TableIndexStatsResponseWithKey,
} from "../../api/indexDetailsApi";
import { generateTableID } from "../../util";
import { DOMAIN_NAME } from "../utils";

export type IndexStatsState = {
  data?: TableIndexStatsResponse;
  lastError?: Error;
  valid: boolean;
  inFlight: boolean;
};

export type IndexStatsReducerState = {
  [id: string]: IndexStatsState;
};

export type ResetIndexUsageStatsPayload = {
  database: string;
  table: string;
};

const initialState: IndexStatsReducerState = {};

const indexStatsSlice = createSlice({
  name: `${DOMAIN_NAME}/indexstats`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<TableIndexStatsResponseWithKey>,
    ) => {
      state[action.payload.key] = {
        data: action.payload.indexStatsResponse,
        valid: true,
        lastError: null,
        inFlight: false,
      };
    },
    failed: (state, action: PayloadAction<ErrorWithKey>) => {
      state[action.payload.key] = {
        data: null,
        valid: false,
        lastError: action.payload.err,
        inFlight: false,
      };
    },
    invalidated: (state, action: PayloadAction<{ key: string }>) => {
      delete state[action.payload.key];
    },
    invalidateAll: state => {
      const keys = Object.keys(state);
      for (const key in keys) {
        delete state[key];
      }
    },
    refresh: (state, action: PayloadAction<TableIndexStatsRequest>) => {
      const key = action?.payload
        ? generateTableID(action.payload.database, action.payload.table)
        : "";
      state[key] = {
        data: null,
        valid: false,
        lastError: null,
        inFlight: true,
      };
    },
    request: (state, action: PayloadAction<TableIndexStatsRequest>) => {
      const key = action?.payload
        ? generateTableID(action.payload.database, action.payload.table)
        : "";
      state[key] = {
        data: null,
        valid: false,
        lastError: null,
        inFlight: true,
      };
    },
    reset: (state, action: PayloadAction<ResetIndexUsageStatsPayload>) => {
      const key = action?.payload
        ? generateTableID(action.payload.database, action.payload.table)
        : "";
      state[key] = {
        data: null,
        valid: false,
        lastError: null,
        inFlight: true,
      };
    },
  },
});

export const { reducer, actions } = indexStatsSlice;
