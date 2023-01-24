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
import { DatabasesListResponse } from "src/api";
import { DOMAIN_NAME } from "../utils";

import { SqlExecutionRequest } from "../../api/sqlApi";

export type DatabasesListState = {
  data: DatabasesListResponse;
  lastError: Error;
  valid: boolean;
};

const initialState: DatabasesListState = {
  data: null,
  lastError: null,
  valid: true,
};

const databasesListSlice = createSlice({
  name: `${DOMAIN_NAME}/databasesList`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<DatabasesListResponse>) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
    },
    refresh: (_, _action: PayloadAction<SqlExecutionRequest>) => {},
    request: (_, _action: PayloadAction<SqlExecutionRequest>) => {},
  },
});

export const { reducer, actions } = databasesListSlice;
