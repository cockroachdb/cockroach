// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  DatabaseDetailsResponse,
  ErrorWithKey,
  SqlApiResponse,
  SqlExecutionErrorMessage,
  TableDetailsReqParams, TableDetailsResponse
} from "../../api";
import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import {DOMAIN_NAME, noopReducer} from "../utils";

type TableDetailsWithKey = {
  tableDetailsResponse: SqlApiResponse<TableDetailsResponse>;
  key: string;
};

export type TableDetailsState = {
  data: SqlApiResponse<TableDetailsResponse>;
  // Captures thrown errors.
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
};

export type KeyedTableDetailsState = {
  cache: {
    [tableID: string]: TableDetailsState;
  };
};

const initialState: KeyedTableDetailsState = {
  cache: {},
};

const tableDetailsReducer = createSlice({
  name: `${DOMAIN_NAME}/tableDetails`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<TableDetailsWithKey>) => {
      state.cache[action.payload.key] = {
        valid: true,
        inFlight: false,
        data: action.payload.tableDetailsResponse,
        lastError: null,
      };
    },
    failed: (state, action: PayloadAction<ErrorWithKey>) => {
      state.cache[action.payload.key] = {
        valid: false,
        inFlight: false,
        data: null,
        lastError: action.payload.err,
      }
    },
    refresh: (_, _action: PayloadAction<TableDetailsReqParams>) => {},
    request: (_, _action: PayloadAction<TableDetailsReqParams>) => {},
  },
});

export const { reducer, actions } = tableDetailsReducer;
