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
import {
  ErrorWithKey,
  StatementDetailsRequest,
  StatementDetailsResponse,
  StatementDetailsResponseWithKey,
} from "src/api/statementsApi";
import { generateStmtDetailsToID } from "../../util";
import moment from "moment-timezone";

export type SQLDetailsStatsState = {
  data: StatementDetailsResponse;
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
  lastUpdated: moment.Moment | null;
};

export type SQLDetailsStatsReducerState = {
  cachedData: {
    [id: string]: SQLDetailsStatsState;
  };
};

const initialState: SQLDetailsStatsReducerState = {
  cachedData: {},
};

const sqlDetailsStatsSlice = createSlice({
  name: `${DOMAIN_NAME}/sqlDetailsStats`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<StatementDetailsResponseWithKey>,
    ) => {
      state.cachedData[action.payload.key] = {
        data: action.payload.stmtResponse,
        valid: true,
        lastError: null,
        inFlight: false,
        lastUpdated: moment.utc(),
      };
    },
    failed: (state, action: PayloadAction<ErrorWithKey>) => {
      state.cachedData[action.payload.key] = {
        data: null,
        valid: false,
        lastError: action.payload.err,
        inFlight: false,
        lastUpdated: moment.utc(),
      };
    },
    invalidated: (state, action: PayloadAction<{ key: string }>) => {
      delete state.cachedData[action.payload.key];
    },
    invalidateAll: state => {
      const keys = Object.keys(state);
      for (const key in keys) {
        delete state.cachedData[key];
      }
    },
    refresh: (state, action: PayloadAction<StatementDetailsRequest>) => {
      const key = action?.payload
        ? generateStmtDetailsToID(
            action.payload.fingerprint_id,
            action.payload.app_names.toString(),
            action.payload.start,
            action.payload.end,
          )
        : "";
      state.cachedData[key] = {
        data: null,
        valid: false,
        lastError: null,
        inFlight: true,
        lastUpdated: null,
      };
    },
    request: (state, action: PayloadAction<StatementDetailsRequest>) => {
      const key = action?.payload
        ? generateStmtDetailsToID(
            action.payload.fingerprint_id,
            action.payload.app_names.toString(),
            action.payload.start,
            action.payload.end,
          )
        : "";
      state.cachedData[key] = {
        data: null,
        valid: false,
        lastError: null,
        inFlight: true,
        lastUpdated: null,
      };
    },
  },
});

export const { reducer, actions } = sqlDetailsStatsSlice;
