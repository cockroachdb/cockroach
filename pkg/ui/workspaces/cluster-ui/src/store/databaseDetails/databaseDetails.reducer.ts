// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  DatabaseDetailsReqParams,
  DatabaseDetailsResponse,
  ErrorWithKey,
  SqlApiResponse,
} from "../../api";
import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { DOMAIN_NAME } from "../utils";

type DatabaseDetailsWithKey = {
  databaseDetailsResponse: SqlApiResponse<DatabaseDetailsResponse>;
  key: string;
};

export type DatabaseDetailsState = {
  data?: SqlApiResponse<DatabaseDetailsResponse>;
  // Captures thrown errors.
  lastError?: Error;
  valid: boolean;
  inFlight: boolean;
};

export type KeyedDatabaseDetailsState = {
  [dbName: string]: DatabaseDetailsState;
};

const initialState: KeyedDatabaseDetailsState = {};

const databaseDetailsReducer = createSlice({
  name: `${DOMAIN_NAME}/databaseDetails`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<DatabaseDetailsWithKey>) => {
      state[action.payload.key] = {
        valid: true,
        inFlight: false,
        data: action.payload.databaseDetailsResponse,
        lastError: null,
      };
    },
    failed: (state, action: PayloadAction<ErrorWithKey>) => {
      state[action.payload.key] = {
        valid: false,
        inFlight: false,
        data: null,
        lastError: action.payload.err,
      };
    },
    refresh: (state, action: PayloadAction<DatabaseDetailsReqParams>) => {
      state[action.payload.database] = {
        valid: false,
        inFlight: true,
        data: null,
        lastError: null,
      };
    },
    request: (state, action: PayloadAction<DatabaseDetailsReqParams>) => {
      state[action.payload.database] = {
        valid: false,
        inFlight: true,
        data: null,
        lastError: null,
      };
    },
  },
});

export const { reducer, actions } = databaseDetailsReducer;
