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
  ErrorWithKey,
  SqlApiResponse,
  TableDetailsReqParams,
  TableDetailsResponse,
} from "../../api";
import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { DOMAIN_NAME } from "../utils";
import { generateTableID } from "../../util";

type TableDetailsWithKey = {
  tableDetailsResponse: SqlApiResponse<TableDetailsResponse>;
  key: string;
};

export type TableDetailsState = {
  data?: SqlApiResponse<TableDetailsResponse>;
  // Captures thrown errors.
  lastError?: Error;
  valid: boolean;
  inFlight: boolean;
};

export type KeyedTableDetailsState = {
  [tableID: string]: TableDetailsState;
};

const initialState: KeyedTableDetailsState = {};

const tableDetailsReducer = createSlice({
  name: `${DOMAIN_NAME}/tableDetails`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<TableDetailsWithKey>) => {
      state[action.payload.key] = {
        valid: true,
        inFlight: false,
        data: action.payload.tableDetailsResponse,
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
    refresh: (state, action: PayloadAction<TableDetailsReqParams>) => {
      state[generateTableID(action.payload.database, action.payload.table)] = {
        valid: false,
        inFlight: true,
        data: null,
        lastError: null,
      };
    },
    request: (state, action: PayloadAction<TableDetailsReqParams>) => {
      state[generateTableID(action.payload.database, action.payload.table)] = {
        valid: false,
        inFlight: true,
        data: null,
        lastError: null,
      };
    },
  },
});

export const { reducer, actions } = tableDetailsReducer;
