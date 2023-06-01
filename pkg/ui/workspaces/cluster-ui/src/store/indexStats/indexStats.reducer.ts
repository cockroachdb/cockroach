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
import { ErrorWithKey } from "../../api";
import { generateTableID } from "../../util";
import {
  TableIndexStatsRequest,
  TableIndexStatsResponse,
  TableIndexStatsResponseWithKey,
} from "../../api/indexDetailsApi";

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
