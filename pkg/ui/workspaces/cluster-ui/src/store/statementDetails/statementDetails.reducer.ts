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

export type SQLDetailsStatsState = {
  data: StatementDetailsResponse;
  lastError: Error;
  valid: boolean;
};

export type SQLDetailsStatsReducerState = {
  [id: string]: SQLDetailsStatsState;
};

const initialState: SQLDetailsStatsReducerState = {};

const sqlDetailsStatsSlice = createSlice({
  name: `${DOMAIN_NAME}/sqlDetailsStats`,
  initialState,
  reducers: {
    received: (
      state,
      action: PayloadAction<StatementDetailsResponseWithKey>,
    ) => {
      state[action.payload.key] = {
        data: action.payload.stmtResponse,
        valid: true,
        lastError: null,
      };
    },
    failed: (state, action: PayloadAction<ErrorWithKey>) => {
      state[action.payload.key] = {
        data: null,
        valid: false,
        lastError: action.payload.err,
      };
    },
    invalidated: (state, action: PayloadAction<{ key: string }>) => {
      if (action.payload.key) {
        state[action.payload.key] = {
          data: null,
          valid: false,
          lastError: null,
        };
      } else {
        const keys = Object.keys(state);
        for (const key in keys) {
          state[key] = {
            data: null,
            valid: false,
            lastError: null,
          };
        }
      }
    },
    refresh: (_, action?: PayloadAction<StatementDetailsRequest>) => {},
    request: (_, action?: PayloadAction<StatementDetailsRequest>) => {},
  },
});

export const { reducer, actions } = sqlDetailsStatsSlice;
