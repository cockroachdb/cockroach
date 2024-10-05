// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  DatabaseDetailsReqParams,
  DatabaseDetailsResponse,
  DatabaseDetailsSpanStatsReqParams,
  DatabaseDetailsSpanStatsResponse,
  ErrorWithKey,
  SqlApiResponse,
} from "../../api";
import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { DOMAIN_NAME } from "../utils";

type DatabaseDetailsWithKey = {
  databaseDetailsResponse: SqlApiResponse<DatabaseDetailsResponse>;
  key: string; // Database name.
};

type DatabaseDetailsSpanStatsWithKey = {
  response: SqlApiResponse<DatabaseDetailsSpanStatsResponse>;
  key: string; // Database name.
};

export type DatabaseDetailsState = {
  data?: SqlApiResponse<DatabaseDetailsResponse>;
  // Captures thrown errors.
  lastError?: Error;
  valid: boolean;
  inFlight: boolean;
};

export type DatabaseDetailsSpanStatsState = {
  data?: SqlApiResponse<DatabaseDetailsSpanStatsResponse>;
  lastError?: Error;
  valid: boolean;
  inFlight: boolean;
};

export type KeyedDatabaseDetailsState = {
  [dbName: string]: DatabaseDetailsState;
};

export type KeyedDatabaseDetailsSpanStatsState = {
  [dbName: string]: DatabaseDetailsSpanStatsState;
};

// const initialState: KeyedDatabaseDetailsState = {};

export const databaseDetailsReducer = createSlice({
  name: `${DOMAIN_NAME}/databaseDetails`,
  initialState: {} as KeyedDatabaseDetailsState,
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

export const databaseDetailsSpanStatsReducer = createSlice({
  name: `${DOMAIN_NAME}/databaseDetailsSpanStats`,
  initialState: {} as KeyedDatabaseDetailsSpanStatsState,
  reducers: {
    received: (
      state,
      action: PayloadAction<DatabaseDetailsSpanStatsWithKey>,
    ) => {
      state[action.payload.key] = {
        valid: true,
        inFlight: false,
        data: action.payload.response,
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
    refresh: (
      state,
      action: PayloadAction<DatabaseDetailsSpanStatsReqParams>,
    ) => {
      state[action.payload.database] = {
        valid: false,
        inFlight: true,
        data: null,
        lastError: null,
      };
    },
    request: (
      state,
      action: PayloadAction<DatabaseDetailsSpanStatsReqParams>,
    ) => {
      state[action.payload.database] = {
        valid: false,
        inFlight: true,
        data: null,
        lastError: null,
      };
    },
  },
});
