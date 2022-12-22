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
import {DOMAIN_NAME, noopReducer} from "../../utils";
import {RecentStatement} from "../../../recentExecutions";
import {RecentStatementsRequestKey, TxnContentionInsightDetailsRequest} from "../../../api";

export type RecentStatementsState = {
  data: RecentStatement[];
  lastError: Error;
  valid: boolean;
};

const initialState: RecentStatementsState = {
  data: null,
  lastError: null,
  valid: true,
};

const recentStatementsSlice = createSlice({
  name: `${DOMAIN_NAME}/recentStatements`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<RecentStatement[]>) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
    },
    // Define actions that don't change state.
    refresh: (
      _,
      _action: PayloadAction<RecentStatementsRequestKey>,
    ) => {},
    request: (
      _,
      _action: PayloadAction<RecentStatementsRequestKey>,
    ) => {},
  },
});

export const { reducer, actions } = recentStatementsSlice;
